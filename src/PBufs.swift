//
//  PBufs.swift
//  protobuf
//
//  Created by Mikhail Opletayev on 6/4/15.
//  Copyright (c) 2015 Mikhail Opletayev. All rights reserved.
//

import Foundation

public class PBDataReader {
    let MaxNestLevel = 80, MaxBlobSize = 0x800000 // +8M
    var _storage: StorageReader
    var _wirefmt: Int32 = 0                 // wire format for current field
    var _level: Int = 0, _fsz: Int = 0      // current field byte size and nesting level
    var _data: Int64 = 0                    // current field value for non-RLE types
    
    
    public init(ds: DataStorage) {
        _storage = StorageReader(ds: ds)
    }
    
    public init(bt: ByteArray, pos: Int, size: Int) {
        _storage = StorageReader(bt: bt, offset: pos, count: size)
    }
    
    // reads message content from Google Protocol Buffers stream.
    public func Read(msg: Message, size: Int) throws {
        if size > 0 {
            _level = 0
            
            // start protobuf stream parsing.
            _storage.Limit = size
            _fsz = size;
            _wirefmt = Pbs.iString;
            try GetMessage(msg, ci: msg.GetDescriptor());
            
            // check if we parsed message to the limit.
            if _storage.Limit > 0 {
                throw DataflowException.PBufsException("incomplete message data")
            }
        }
        else if size < 0 {
            throw DataflowException.InvalidArgumentException("size must be positive")
        }
    }
    
    func FormatError(wt: Int32) throws -> Int64 {
        if wt >= 0 {
            throw DataflowException.PBufsException("type mismatch: expected " + Pbs.GetWireTypeName(wt) + ", actual " + Pbs.GetWireTypeName(_wirefmt))
        }
        return 0
    }
    
    func GetVarInt() throws -> Int64 {
        return _wirefmt == Pbs.iVarInt ? _data : try FormatError(Pbs.iVarInt);
    }
    
    func GetBits64(expected: Int32) throws -> Int64 {
        return _wirefmt == expected ? _data : try FormatError(expected);
    }
    
    func GetMessage(msg: Message, ci: MessageDescriptor) throws {
        // enter next message parsing level.
        let prev = try PushLimit()
        if _storage.Limit > 0 {
            // read next PB value from the stream.
            let id = try _storage.GetIntPB()
            _wirefmt = id & 0x7
            switch _wirefmt {
            case Pbs.iVarInt:
                _data = try _storage.GetLongPB()
                break
            case Pbs.iBit64:
                _data = try _storage.GetB64()
                break
            case Pbs.iString:
                _fsz = Int(try _storage.GetIntPB())
                if _fsz <= _storage.Limit { break }
                throw DataflowException.PBufsException("nested blob size")
            case Pbs.iBit32:
                _data = Int64(try _storage.GetB32())
                break
            default:
                throw  DataflowException.PBufsException("unsupported wire format");
            }
            
            // match PB field descriptor by id.
            if let fi = ci.Find(id) {
                if fi.Id == id {
                    msg.Get(fi, rdr: self)
                } else {
                    try TryReadPacked(fi, msg: msg)
                }
            } else {
                if _fsz > 0 {
                    _storage.Skip(_fsz)
                    _fsz = 0
                }
            }
            
            // exit message segment parsing.
            if _storage.Limit < 0 {
                throw DataflowException.PBufsException("message size out of sync")
            }
            _level -= 1
            PopLimit(prev)
        }
    }
    
    
    private func PopLimit(prev: Int) {
        _storage.Limit = prev
    }
    
    private func PushLimit() throws -> Int {
        let prev = _storage.Limit
        if _fsz <= prev { _storage.Limit = _fsz }
        else { throw DataflowException.PBufsException("nested limit out of bounds") }
        _fsz = 0
        return prev - _storage.Limit
    }
    
    public func TryReadPacked(fs: FieldDescriptor, msg: Message) throws {
        let wireFmt = fs.Id & 0x7
        if _wirefmt == Pbs.iString {
            _wirefmt = wireFmt
        } else {
            try FormatError(wireFmt)
        }
        let prev = try PushLimit()
        while _storage.Limit > 0 {
            switch _wirefmt {
            case Pbs.iVarInt:
                _data = try _storage.GetLongPB()
                break
            case Pbs.iBit64:
                _data = try _storage.GetB64()
                break
            case Pbs.iBit32:
                _data = Int64(try _storage.GetB32())
                break
            default:
                throw DataflowException.PBufsException("packed: types")
            }
            msg.Get(fs, rdr: self)
        }
        PopLimit(prev)
    }
}

extension PBDataReader: IDataReader {
    public func AsBit32() throws -> Int32 {
        return Int32(try GetBits64(Pbs.iBit32))
    }
    
    public func AsBit64() throws -> Int64 {
        return try GetBits64(Pbs.iBit64)
    }
    
    public func AsBool() throws -> Bool {
         return try GetVarInt() != 0
    }
    
    public func AsBytes() throws -> ByteArray? {
        if _wirefmt == Pbs.iString {
            if _fsz == 0 { return nil }
            if _fsz <= MaxBlobSize {
                let bt = ByteArray(count: _fsz)
                return try _storage.GetBytes(bt, btSize: &_fsz)
            }
            throw DataflowException.PBufsException("bytes size")
        }
        try FormatError(Pbs.iString)
        return nil
    }
    
    public func AsChar() throws -> UTF16Char {
        return UTF16Char(try GetVarInt())
    }
    
    public func AsCurrency() throws -> Currency {
        return Currency(long: try GetVarInt())
    }
    
    public func AsDouble() throws -> Double {
        return Pbs.SetDoubleBits(try GetBits64(Pbs.iBit64));
    }
    
    public func AsEnum(es: EnumDescriptor) throws -> Int32
    {
        let ev = Int32(try GetVarInt())
        // todo: should we check if it is valid enum value.
        return ev
    }
    
    public func AsFloat() throws -> Float {
        return Pbs.SetFloatBits(Int32(try GetBits64(Pbs.iBit32)));
    }
    
    public func AsInt() throws  -> Int32 {
        return Int32( try GetVarInt())
    }
    
    public func AsLong() throws -> Int64 {
        return try GetVarInt()
    }
    
    public func AsMessage(msg: Message, fs: FieldDescriptor) throws {
        if _wirefmt == Pbs.iString {
            if _fsz == 0 { return }
            _level += 1
            if _level < MaxNestLevel {
                try GetMessage(msg, ci: fs.MessageType!)
            }
            else {
                throw DataflowException.PBufsException("message nesting too deep")
            }
        }
        else {
            try FormatError(Pbs.iString)
        }
    }
    
    public func AsString() throws -> String {
        if _wirefmt != Pbs.iString {
            try FormatError(Pbs.iString)
        }
        if _fsz == 0 { return "" }
        if _fsz > MaxBlobSize { throw DataflowException.PBufsException("string size") }
        var pos = _fsz
        let bt = try _storage.GetBytes(nil, btSize: &pos)
        let mem = bt._mem + pos
        let str = NSString(bytes: mem, length: _fsz, encoding: NSUTF8StringEncoding)
        _fsz = 0 
        return str as! String
    }
    
    public func AsSi32() throws -> Int32 {
        let i = Int32(try GetVarInt())
        return (i >> 1) ^ -(i & 1)
    }
    
    public func AsSi64() throws -> Int64 {
        let i = try GetVarInt()
        return (i >> 1) ^ -(i & 1)
    }
    
}

public class PBDataWriter {
    let _storage: StorageWriter
    
    public var Storage: StorageWriter { get { return _storage } }
    
    public init(sw: StorageWriter) {
        _storage = sw
    }
    
    public init(bts: ByteArray, pos: Int, count: Int) {
        _storage = StorageWriter(db: bts, pos: pos, count: count)
    }
    
    public func AppendMessage(message: Message, ci: MessageDescriptor) throws -> PBDataWriter {
        // force recalc on _memoized_size to guarantee up-to-date value.
        message.GetSerializedSize()
        // serialize message fields.
        try message.Put(self)
        return self
    }
    
    public func WriteInt(fs: FieldDescriptor, i: Int32) throws {
       try _storage.WriteIntPB(fs.Id, i2: i)
    }
    
    public func WriteLong(fs: FieldDescriptor, l: Int64) throws {
        try _storage.WriteIntPB(fs.Id)
        try _storage.WriteLongPB(l)
    }
    
    public func WriteMessage(fs: FieldDescriptor, msg: Message?) throws {
        if let msg = msg {
            try _storage.WriteIntPB(fs.Id, i2: msg.SerializedSize)
            try msg.Put(self)
        }
    }
    
    public func WriteString(fs: FieldDescriptor, s: String) throws {
        try _storage.WriteIntPB(fs.Id, i2: Int32(Pbs.GetUtf8ByteSize(s)))
        try _storage.WriteString(s)
    }
}

extension PBDataWriter: IDataWriter {
    private func AsRepeatedPacked(fs: FieldDescriptor, data: AnyObject) throws {
        try _storage.WriteIntPB((fs.Id & ~0x7) | Pbs.iString)
        var sz: Int32 = 0
        switch fs.DataType {
        case WireType.Enum,
             WireType.Int32:
            let ia = data as? ArrayRef<Int32>
            if let ia = ia {
                if !fs.IsSignedInt {
                    for x in ia.value { sz += Pbs.i32(x) }
                    try _storage.WriteIntPB(sz)
                    for x in ia.value { try _storage.WriteIntPB(x) }
                } else {
                    for x in ia.value { sz += Pbs.si32(x) }
                    try _storage.WriteIntPB(sz);
                    for x in ia.value { try _storage.WriteIntPB((x << 1) ^ (x >> 31)) }
                }
            } else { throw DataflowException.PBufsException("packed: invalid array") }
            break
        case WireType.Bit32:
            let b4 = data as? ArrayRef<Int32>
            if let b4 = b4 {
                try _storage.WriteIntPB(Int32(b4.count * 4))
                for x in b4.value { try _storage.WriteB32(UInt32(x)) }
            } else { throw DataflowException.PBufsException("packed: invalid bit32") }
            break
        case WireType.Int64:
            let la = data as? ArrayRef<Int64>
            if let la = la {
                if !fs.IsSignedInt {
                    for x in la.value { sz += Pbs.i64(x) }
                    try _storage.WriteIntPB(sz)
                    for x in la.value { try _storage.WriteLongPB(x) }
                } else {
                    for x in la.value { sz += Pbs.si64(x) }
                    try _storage.WriteIntPB(sz)
                    for x in la.value { try _storage.WriteLongPB((x << 1) ^ (x >> 63)) }
                }
            } else { throw DataflowException.PBufsException("packed: invalid int64") }
            break
        case WireType.Bit64:
            let b8 = data as? ArrayRef<Int64>
            if let b8 = b8 {
                try _storage.WriteIntPB(Int32(b8.count * 8))
                for x in b8.value { try _storage.WriteB64(UInt64(x)) }
            } else { throw DataflowException.PBufsException("packed: invalid bit64") }
            break
        case WireType.Bool:
            let bla = data as? ArrayRef<Bool>
            if let bla = bla {
                try _storage.WriteIntPB(Int32(bla.count))
                for x in bla.value { try _storage.WriteIntPB(x ? 1 : 0) }
            } else { throw DataflowException.PBufsException("packed: invalid bool") }
            break
        case WireType.Char:
            let ch = data as? ArrayRef<UTF16Char>
            if let ch = ch {
                for x in ch.value { sz += Pbs.i32(Int32(x)) }
                try _storage.WriteIntPB(sz)
                for x in ch.value { try _storage.WriteIntPB(Int32(x)) }
            } else { throw DataflowException.PBufsException("packed: invalid char") }
            break
        case WireType.Currency:
            let cra = data as? ArrayRef<Currency>
            if let cra = cra {
                for x in cra.value { sz += Pbs.i64(x.Value) }
                try _storage.WriteIntPB(sz)
                for x in cra.value { try _storage.WriteLongPB(x.Value) }
            } else { throw DataflowException.PBufsException("packed: invalid currency") }
            break
//        case WireType.Date:
//            var dta = data as DateTime[];
//            if (dta == null) goto default;
//            foreach (var x in dta) sz += Pbs.dat(x);
//            _storage.WriteIntPB(sz);
//            foreach (var x in dta) WriteDate(x);
//            break;
        case WireType.Double:
            let da = data as? ArrayRef<Double>
            if let da = da {
                try _storage.WriteIntPB(Int32(da.count * 8))
                for x in da.value { try _storage.WriteB64(UInt64(Pbs.GetDoubleBits(x))) }
            } else { throw DataflowException.PBufsException("packed: invalid double") }
            break
        case WireType.Float:
            let fa = data as? ArrayRef<Float>
            if let fa = fa {
                try _storage.WriteIntPB(Int32(fa.count * 4))
                for x in fa.value { try _storage.WriteB32(UInt32(Pbs.GetFloatBits(x))) }
            } else { throw DataflowException.PBufsException("packed: invalid float") }
            break
        default:
            throw DataflowException.PBufsException("packed: invalid element type")
        }
    }
    
    private func AsRepeatedArray(fs: FieldDescriptor, data: AnyObject) throws {
        let writer: IDataWriter = self
        switch fs.DataType {
        case WireType.Enum,
             WireType.Int32:
            let id = data as? ArrayRef<Int32>
            if let id = id {
                if !fs.IsSignedInt {
                    for x in id.value { try WriteInt(fs, i: x) }
                } else {
                    for x in id.value { try writer.AsSi32(fs, i: x) }
                }
            } else { throw DataflowException.PBufsException("repeated array: invalid int32") }
            break
        case WireType.Bit32:
            let id = data as? ArrayRef<Int32>
            if let id = id {
                for x in id.value { try writer.AsBit32(fs, i: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid bit32") }
            break
        case WireType.Int64:
            let id = data as? ArrayRef<Int64>
            if let id = id {
                if !fs.IsSignedInt {
                    for x in id.value { try WriteLong(fs, l: x) }
                } else {
                    for x in id.value { try writer.AsSi64(fs, l: x) }
                }
            } else { throw DataflowException.PBufsException("repeated array: invalid int64") }
            break
        case WireType.Bit64:
            let id = data as? ArrayRef<Int64>
            if let id = id {
                for x in id.value { try writer.AsBit64(fs, l: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid bit64") }
            break
        case WireType.Bool:
            let id = data as? ArrayRef<Bool>
            if let id = id {
                for x in id.value { try WriteInt(fs, i: x ? 1 : 0) }
            } else { throw DataflowException.PBufsException("repeated array: invalid bool") }
            break
        case WireType.Char:
            let id = data as? ArrayRef<UTF16Char>
            if let id = id {
                for x in id.value { try WriteInt(fs, i: Int32(x)) }
            } else { throw DataflowException.PBufsException("repeated array: invalid char") }
            break
        case WireType.Currency:
            let id = data as? ArrayRef<Currency>
            if let id = id {
                for x in id.value { try WriteLong(fs, l: x.Value) }
            } else { throw DataflowException.PBufsException("repeated array: invalid currency") }
            break
//        case WireType.Date:
//            foreach (var x in data as DateTime[]) writer.AsDate(fs, x);
//            break;
        case WireType.Double:
            let id = data as? ArrayRef<Double>
            if let id = id {
                for x in id.value { try writer.AsDouble(fs, d: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid double") }
            break
        case WireType.Float:
            let id = data as? ArrayRef<Float>
            if let id = id {
                for x in id.value { try writer.AsFloat(fs, f: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid float") }
            break
        case WireType.String:
            let id = data as? ArrayRef<String>
            if let id = id {
                for x in id.value { try writer.AsString(fs, s: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid string") }
            break
        case WireType.Bytes:
            let id = data as? ArrayRef<ByteArray>
            if let id = id {
                for x in id.value { try writer.AsBytes(fs, bt: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid bytes") }
            break
        case WireType.Message:
            let id = data as? ArrayRef<Message>
            if let id = id {
                for x in id.value { try writer.AsMessage(fs, msg: x) }
            } else { throw DataflowException.PBufsException("repeated array: invalid message") }
            break
//        case WireType.MapEntry:
//            foreach (var x in data as MapEntry[]) WriteMessage(fs, x);
//            break;
        default:
            throw DataflowException.PBufsException("repeated array: invalid element type")
        }
    }
    
    public func AsRepeated(fs: FieldDescriptor, data: AnyObject) throws {
        if !fs.IsPacked {
            try AsRepeatedArray(fs, data: data)
        } else {
            try AsRepeatedPacked(fs, data: data)
        }
    }
    
    public func AsBit32(fs: FieldDescriptor, i: Int32) throws {
        try _storage.WriteIntPB(fs.Id);
        try _storage.WriteB32(UInt32(i));
    }
    
    public func AsBit64(fs: FieldDescriptor, l: Int64) throws {
        try _storage.WriteIntPB(fs.Id);
        try _storage.WriteB64(UInt64(l));
    }
    
    public func AsBool(fs: FieldDescriptor, b: Bool) throws {
        try WriteInt( fs, i: b ? 1 : 0)
    }
    
    public func AsChar(fs: FieldDescriptor, ch: UTF16Char) throws {
        try WriteInt(fs, i: Int32(ch))
    }
    
    public func AsBytes(fs: FieldDescriptor, bt: ByteArray) throws {
        try _storage.WriteIntPB(fs.Id);
        let sz = bt.count;
        try _storage.WriteIntPB(Int32(sz));
        if sz != 0 { try _storage.WriteBytes(sz, bt: bt) }
    }
    
    public func AsCurrency(fs: FieldDescriptor, cy: Currency) throws {
        try WriteLong(fs, l: cy.Value)
    }
    
//    public override void AsDate(FieldDescriptor fs, DateTime dt)
//    {
//      _storage.WriteIntPB(fs.Id);
//      WriteDate(dt);
//    }
    
    public func AsDouble(fs: FieldDescriptor, d: Double) throws {
        try _storage.WriteIntPB(fs.Id);
        try _storage.WriteB64(UInt64(Pbs.GetDoubleBits(d)))
    }
    
    public func AsEnum(fs: FieldDescriptor, en: Int32) throws {
        try WriteInt(fs, i: en)
    }
    
    public func AsInt(fs: FieldDescriptor, i: Int32) throws {
        try WriteInt(fs, i: i)
    }
    
    public func AsLong(fs: FieldDescriptor, l: Int64) throws {
        try WriteLong(fs, l: l)
    }
    
    public func AsFloat(fs: FieldDescriptor, f: Float) throws {
        try _storage.WriteIntPB(fs.Id);
        try _storage.WriteB32(UInt32(Pbs.GetFloatBits(f)))
    }
    
    public func AsMessage(fs: FieldDescriptor, msg: Message?) throws {
        try WriteMessage(fs, msg: msg)
    }
    
    public func AsString(fs: FieldDescriptor, s: String) throws {
        try WriteString(fs, s: s)
    }
    
    public func AsSi32(fs: FieldDescriptor, i: Int32) throws {
        try WriteInt(fs, i: (i << 1) ^ (i >> 31))
    }
    
    public func AsSi64(fs: FieldDescriptor, l: Int64) throws {
        try WriteLong(fs, l: (l << 1) ^ (l >> 63))
    }
    
    public func IsNull(fs: FieldDescriptor) {
        // TODO: WTF?
    }
}


