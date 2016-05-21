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
    var _wirefmt: Int           // wire format for current field
    var _level: Int, _fsz: Int    // current field byte size and nesting level
    var _data: Int64;             // current field value for non-RLE types
    
    
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
    
    func FormatError(wt: Int) throws -> Int64 {
        if wt >= 0 {
            throw DataflowException.PBufsException("type mismatch: expected " + Pbs.GetWireTypeName(wt) + ", actual " + Pbs.GetWireTypeName(_wirefmt))
        }
        return 0
    }
    
    func GetVarInt() throws -> Int64 {
        return _wirefmt == Pbs.iVarInt ? _data : try FormatError(Pbs.iVarInt);
    }
    
    func GetBits64(expected: Int) throws -> Int64 {
        return _wirefmt == expected ? _data : try FormatError(expected);
    }
    
    func GetMessage(msg: Message, ci: MessageDescriptor) throws {
        // enter next message parsing level.
        let prev = try PushLimit()
        if _storage.Limit > 0 {
            // read next PB value from the stream.
            let id = Int(try _storage.GetIntPB())
            _wirefmt = Int(id & 0x7)
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
                throw DataflowException.PBufsException("nested blob size");
                break
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
    
    public func AsEnum(es: EnumDescriptor) throws -> Int
    {
        let ev = Int(try GetVarInt())
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


