//
//  Message.swift
//  protobuf
//
//  Created by Mikhail Opletayev on 6/3/15.
//  Copyright (c) 2015 Mikhail Opletayev. All rights reserved.
//

import Foundation

// Protocol Buffers message field kinds, as defined in the Google standard specification.
public enum FieldKind: Int {
    case Unknown    = 0
    case Required   = 1
    case Optional   = 2
    case Repeated   = 3
    case Enum       = 4
    case Map        = 5
}

 // Lists all built-in types supported for message fields. Includes extensions to the Google specification, like Currency, Date and Decimal.
public enum DataType: Int {
    case Int32      = 0
    case Int64      = 1
    case UInt32     = 2
    case UInt64     = 3
    case SInt32     = 4
    case SInt64     = 5
    case Bool       = 6
    case Float      = 7
    case Double     = 8
    case Fixed32    = 9
    case Fixed64    = 10
    case SFixed32   = 11
    case SFixed64   = 12
    case Bytes      = 13
    case String     = 14
    case Enum       = 15
    case Message    = 16
    case Date       = 17
    case Decimal    = 18
    case Object     = 19
    case Currency   = 20
    case MapEntry   = 21
    case Undefined  = 22
    case LastIndex  = 23
}

// lists all supported .NET types for native DataReader/Writer operations.
// if any existing values are changed, all client proto files should be recompiled.
public enum WireType: Int
{
    case None       = 0
    case Int32      = 1
    case Int64      = 2
    case Sint32     = 3
    case Sint64     = 4
    case String     = 5
    case Bytes      = 6
    case Date       = 7
    case Decimal    = 8
    case Bit32      = 9
    case Bit64      = 10
    case Float      = 11
    case Double     = 12
    case Bool       = 13
    case Char       = 14
    case Message    = 15
    case Enum       = 16
    case Currency   = 17
    case MapEntry   = 18
    case MaxValue   = 20
    
    public func WireFormat() -> Int {
        switch self {
            case
            .Int32,
            .Int64,
            .Sint32,
            .Sint64,
            .Bool,
            .Char,
            .Date,
            .Currency,
            .Enum:
                return Pbs.iVarInt;
            case
            .String,
            .Bytes,
            .Decimal,
            .Message,
            .MapEntry:
                return Pbs.iString;
            case
            .Bit32,
            .Float:
                return Pbs.iBit32;
            case
            .Bit64,
            .Double:
                return Pbs.iBit64;
            default:
                return Pbs.iBadCode
        }
    }
}

// list of stream data formats implemented by streaming RPC channels.
public enum DataEncoding: Int
{
    case Unknown = 0
    case Proto = 1
    case Json = 2
    case QueryString = 4
    case Xml = 8
    case Memcached = 16
    case Any = 0x1F
}

// In-place, mutable, generic-typed list, implements repeated fields in messages.
// Similar to .NET generic List<T> in functionality, but implemented as struct to avoid extra allocations on message creation.
public struct Repeated<T where T:Equatable> {
    private var items: [T] = []
    
    init(s: [T]) {
        items = s
    }
    
    var Count: Int {
        get { return items.count }
    }
    
    var Items: [T]
    {
        get { return items }
        set { items = newValue }
    }
    
    var AsArray: [T] { get { return items } }
    
    var IsEmpty: Bool { get { return items.count == 0 } }
    
    var FirstOrDefault: T? {
        get {
            return (items.count == 0 ? nil : items.first)
        }
    }
    
    subscript(index: Int) -> T {
        get { return items[index] }
        set { items[index] = newValue }
    }
    
    mutating func Add(i: T) -> T {
        items.append(i)
        return i
    }
    
    mutating func Add(arr: [T]) {
        items += arr
    }
    
    mutating func Add(arr: [T], pos:Int, count:Int) {
        if count == 0 { return }
        
        if items.count == 0 && arr.count == count {
            items = arr
            return
        }
        items.insertContentsOf(arr[0..<count], at:pos)
    }
    
    mutating func Clear() {
        items.removeAll(keepCapacity: false)
    }
    
    mutating func Remove(item: T) {
        items = items.filter({$0 != item})
    }
    
    func ToArray()->[T] {
        return items
    }
}

// Value Storage structure
public enum ValueStorage {
    case _bool(Bool)
    case _int32(Int)
    case _int64(Int64)
    case _single(Float)
    case _double(Double)
    case _byte(Byte)
}

// Exception classes for Dataflow libraries.
public enum DataflowException: ErrorType {
    case GenericException(String)
    case SerializationException(String)
    case WriteOnceException
    case NotImplementedException
    case KeyNotFoundException(String)
    case InvalidValueException
    case InvalidOperationException
    case OutOfRangeException
    case StorageState
    case PBufsException(String)
    case InvalidArgumentException(String)
    
    static let MustOverride = "This function must be overridden"
}


// efficient long-int based type for currency values manipulations.
public struct Currency: IntegerLiteralConvertible, Equatable, Comparable {
    public static let Scale: Int64 = 10000
    public static var Zero: Currency { return Currency(long:0) }
    
    private var value: Int64
    
    public init(long:Int64) {
        value = long
    }
    public init(units:Int64, cents:Int=0) {
        value = units * Currency.Scale + cents
    }
    public init(float:Double) {
        value = Int64(float*Double(Currency.Scale))
    }
    public init(integerLiteral value:IntegerLiteralType) {
        self.init(long: Int64(value))
    }
    public init(cur:Currency) {
        self.init(long:cur.Value)
    }
    public init() {
        value = 0
    }
    
    public var Units: Int64 {
        get {
            return value / Currency.Scale;
        }
        set {
            value = newValue * Currency.Scale;
        }
    }
    
    public var Cents: Int64 {
        get {
            return value % Currency.Scale
        }
    }
    
    public var Value: Int64 {
        get {
            return value
        }
        set {
            value = newValue
        }
    }
    
    public func Abs() -> Currency {
        return value > 0 ? Currency(long: value): Currency(long: -1*value)
    }
    
    public func Max(c: Currency) -> Currency {
        return self > c ? self: c
    }
    
    public func ToDouble() -> Double {
        return Double(value)/Double(Currency.Scale)
    }
    
    public func ToInt() -> Int {
        return Int(value/Currency.Scale)
    }
    
    public mutating func TryParse(s: String) -> Bool {
        var uc = 0, i = 0, sign = 0
        
        for ch in s.characters {
            if ch < "0" || ch > "9" {
                if i == 0 {
                    if ch == "-" { sign = 1 }
                    else if ch == "+" { sign = 2 }
                    else { return false }
                }
                else {
                    if ch == "." && uc == 0 { uc = i }
                    else { return false }
                }
            }
            i += 1
        }
        
        var cents: Int = 0
        
        if uc != 0 {
            var k = uc + 1
            while k < i { cents = cents * 10 + Int(s[k] - 48); k += 1 } // MO: s[k] here is slow, Utils
            while k - uc < 5 { cents *= 10; k += 1 }
            i = uc
        }
        
        var units: Int64 = 0
        uc = sign == 0 ? 0 : 1
        
        while uc < i {
            units = units * 10 + Int64(s[uc] - 48) // MO: s[uc] is slow here, see Utils
            uc += 1
        }
        
        value = units * Currency.Scale + Int64(cents)
        if sign == 1 { value = -value }
        
        return true
    }
    
    public func ToString() -> String {
        var str = "\(value)"
        let ns = str as NSString
        var sz = ns.length
        if sz > 4 {
            let unit: String = ns.substringToIndex(sz-4)
            let cent: String = ns.substringFromIndex(sz-4)
            return "\(unit).\(cent)"
        }
        while sz < 4 { str = "0" + str; sz += 1 }
        return "0." + str
    }
    
}

public func + (c1:Currency, c2:Currency) -> Currency {
    return Currency(long: c1.Value + c2.Value)
}

public func - (c1:Currency, c2:Currency) -> Currency {
    return Currency(long: c1.Value - c2.Value)
}

public func * (c1:Currency, i1: Int) -> Currency {
    return Currency(long: c1.Value*Int64(i1))
}

public func == (c1: Currency, c2: Currency) -> Bool {
    return c1.Value == c2.Value
}

public func != (c1:Currency, c2: Currency) -> Bool {
    return c1.Value != c2.Value
}

public func > (c1:Currency, c2:Currency) -> Bool {
    return c1.Value > c2.Value
}

public func < (c1:Currency, c2:Currency) -> Bool {
    return c1.Value < c2.Value
}

public func >= (c1:Currency, c2:Currency) -> Bool {
    return c1.Value >= c2.Value
}

public func <= (c1:Currency, c2:Currency) -> Bool {
    return c1.Value <= c2.Value
}

// Static class for helper methods mostly related to Google Protocol Buffers encoding.
public class Pbs {
    public static let // PB specification wire data encoding codes
        iVarInt = 0, iBit64 = 1, iString = 2, iStartGroup = 3, iEndGroup = 4, iBit32 = 5, iBadCode = 8
    
    public static let // FieldOpts const values must not be changed.
        iNone = 0, iBox = 2, iList = 1, iSpecial = 4, iKvMap = 8, iGoogleType = 16,
        szFixed32 = 4, szFixed64 = 8, szFloat = 4, szDouble = 8, szBool = 1
 
    public static func i32(val:Int32) -> Int {
        let i = UInt32(val)
        if i < 0x80 { return 1 }
        if i < 0x4000 { return 2 }
        if i < 0x200000 { return 3 }
        if i < 0x10000000 { return 4 }
        return i > 0 ? 5 : 10
    }
    
    public static func i64(val: Int64) -> Int {
        let i = UInt64(val)
        if (i >> 31) == 0 { return Pbs.i32(Int32(val)) }
        if i < 0x800000000 { return 5 }
        if i < 0x40000000000 { return 6 }
        if i < 0x2000000000000 { return 7 }
        if i < (UInt64(1) << 56) { return 8 }
        return i < (UInt64(1) << 63) ? 9 : 10;
    }
    
    public static func sign(v: Int32) -> Int { return Int((v << 1) ^ (v >> 32)) }
    public static func signl(v: Int64) -> Int { return Int((v << 1) ^ (v >> 63)) }
    public static func sign(v: Int) -> Int { return Int(Pbs.signl(Int64(v))) }
    public static func si32(v: Int32) -> Int { return Pbs.i32((v << 1) ^ (v >> 32)) }
    public static func si64(v: Int64) -> Int { return Pbs.i64((v << 1) ^ (v >> 63)) }
    public static func bln(v: Bool) -> Int { return 1 }
    public static func chr(v: UTF8Char) -> Int { return v < 128 ? 1 : 2 }
    public static func str(s: String) -> Int { return Pbs.szPfx(s.characters.count) }
    public static func szPfx(sz: Int) -> Int { return sz + Pbs.i32(Int32(sz)) }
    public static func bts(b: [Byte]) -> Int { return Pbs.szPfx(b.count) }
    public static func msg(msg: Message) -> Int { return Pbs.szPfx(msg.GetSerializedSize()) }
    public static func cur(cy: Currency) -> Int { return Pbs.i64(cy.Value) }
    
    public static func dec2(di: [Int32]) -> Int {
        let si: Int32 = di[3]
        var sg : Int32 = si < 0 ? 1 : 0;
        if si == 0 && di[0] == 0 && di[1] == 0 && di[2] == 0 { return 1 }
        sg = sg | Int32(Byte((si >> 16) << 1))
        var sz = Pbs.i32(di[0])
        var li = Int64(di[2])
        li = li << 32
        sz += Pbs.i64(li | Int64(di[1])) // MO: sz += i64(li | (uint)di[1]);
        sz += Pbs.i32(sg)
        return sz + 1
    }
    
    public static func EqualBytes(inout a: [Byte], inout b: [Byte]) -> Bool {
        return a == b
    }
    
    public static func SwapBytes(i: UInt32) -> UInt32 {
        return (i >> 24) | ((i & 0xFF0000) >> 8) | (i << 24) | ((i & 0xFF00) << 8)
    }
    
    public static func GetWireId(id: Int, wt: WireType) -> Int {
        return (id << 3) | wt.WireFormat();
    }
    
    // Gets number of unicode chars encoded into UTF8 byte array.
    public static func GetUtf8CharSize(bt: [Byte]) -> Int {
        var i = 0, k = 0
        for b in bt {
            if k > 0 { k -= 1; continue }
            i += 1
            if b < 0x80 { continue }
            k = b < 0xE0 ? 1 : 2
        }
        return i
    }

    // String size in UTF8 encoding, uses simplified checks
    public static func GetUtf8ByteSize(s: String) -> Int {
        return s.lengthOfBytesUsingEncoding(NSUTF8StringEncoding) // MO: needs tesitng
    }
    
    public static func GetDoubleBits(dv: Double) -> Int64 {
        return Unsafe.toInt64Bits(dv)
    }
    
    public static func SetDoubleBits(lv: Int64) -> Double {
        return Unsafe.fromInt64Bits(lv, Double.self)
    }
    
    public static func GetFloatBits(dv: Float) -> Int32 {
        return Unsafe.toInt32Bits(dv)
    }
    
    public static func SetFloatBits(lv: Int32) -> Float {
        return Unsafe.fromInt32Bits(lv, Float.self)
    }

    // Returns text name for PB encoding markers.
    public static func GetWireTypeName(id: Int) -> String {
        switch id {
        case iVarInt: return "varint";
        case iString: return "string";
        case iBit64: return "64-bit";
        case iStartGroup,
             iEndGroup: return "group(deprecated)";
        case iBit32: return "32-bit";
        default: return "undefined";
        }
    }
    
    // Encode/decode helpers
    public static func VarInt64Ex(db: ByteArray, inout pos: Int, b0: UInt32) throws -> UInt64 {
        var b0 = b0
        var ul: UInt64 = 0
        var i = pos // MO: for some reason this is casted to UInt
        var b: UInt32 = UInt32(db[i])
        b0 = (b0 & 0x3FFF) | UInt32(b << 14)
        while b > 0x7F {
            i = i + 1
            b0 = (b0 & UInt32(0x1FFFFF)) | UInt32(b = UInt32(db[i]) << 21)
            if b < 0x80 { break }
            b0 &= 0xFFFFFFF
            i = i + 1
            var b1: UInt32 = UInt32(db[i]), b2: UInt32 = 0;
            while b1 > 0x7F {
                i = i + 1
                b1 = (b1 & UInt32(0x7F)) | UInt32(b = UInt32(db[i]) << 7)
                if b < 0x80 { break }
                i = i + 1
                b1 = (b1 & UInt32(0x3FFF)) | UInt32(b = UInt32(db[i]) << 14)
                if b < 0x80 { break }
                i = i + 1
                b1 = (b1 & UInt32(0x1FFFFF)) | UInt32(b = UInt32(db[i]) << 21)
                if b < 0x80 { break }
                b1 &= 0xFFFFFFF
                i = i + 1
                b2 = UInt32(b = UInt32(db[i]))
                if b < 0x80 { break }
                i = i + 1
                b2 = (b2 & UInt32(0x7F)) | UInt32(b = UInt32(db[i]) << 7)
                if b > 127 { throw DataflowException.SerializationException("varint value is too long") }
                break;
            }
            ul = (UInt64(b1) << 28) | (UInt64(b2) << 56)
            break
        }
        
        i = i + 1
        pos = i
        return ul | UInt64(b0)
    }
    
    public static func VarInt64(db: ByteArray, inout pos: Int) throws -> UInt64 {
        var i = pos
        var b0: UInt32 = UInt32(db[i])
        i = i + 1
        if b0 < 0x80 { return UInt64(b0) }
        b0 = (b0 & UInt32(0x7F)) | (UInt32(db[i]) << 7)
        i = i  + 1
        pos = i
        return (b0 < UInt32(0x4000) ? UInt64(b0) : try VarInt64Ex(db, pos: &pos, b0: b0))
    }
    
    public static func PutString(inout db: ByteArray, s: String, i: Int) -> Int {
        var i = i
        for c in s.utf16 {
            if c < 0x80 {
                db[i] = Byte(c)
                i = i + 1
            }
            else {
                if c < 0x800 {
                    db[i] = Byte(0xC0 | (c >> 6))
                    i = i + 1
                }
                else {
                    db[i] = Byte(0xE0 | ((c >> 12) & 0x0F))
                    i = i + 1
                    db[i] = Byte(0x80 | ((c >> 6) & 0x3F))
                    i = i + 1
                }
                db[i] = Byte(0x80 | (c & 0x3F))
                i = i + 1
            }
        }
        
        return i
    }
    
    public static func Align08(i: Int32) -> Int32 { return (i + 7) & (~0x7) }
    public static func Align16(i: Int32) -> Int32 { return (i + 15) & (~0xf) }
}

// The root for all message classes generated by Dataflow Protocol Buffers Compiler for C#.
// Declares virtual methods that are implemented by compiler in produced classes.
public class Message {
    public static let ClassName = "Message"
    public static let Empty = Message()
    
    private static let _desc_ = MessageDescriptor_20(name: "message", options: 0, factory: Message.Empty)
    
    // cache for message size and bitmask
    var _memoized_size: Int = 0
    
    // methods that are implemented by .proto compiler.
    // returns metadata object that helps runtime libraries in working with message content.
    public func  GetDescriptor() -> MessageDescriptor { return Message._desc_ }
    
    // calculates message size in bytes in the Google Protocol Buffers format.
    public func GetSerializedSize() -> Int { return 0 }
    
    // clears the message contents. All fields are marked as null, and set to the values specified in proto file or to the type defaults.
    public func Clear() { _memoized_size = 0 }
    
    // reads value from data reader into the message field.
    public func Get(fs: FieldDescriptor, rdr: IDataReader) { }
    public func Get(fs: FieldDescriptor, rdr: TDataReader) { }
    
    // returns true if all required fields in the message and all embedded messages are set, false otherwise.
    public func IsInitialized() -> Bool { return true }
    
    // fast self-factory implementation.
    public func New() -> Message { return Message.Empty; }
    
    // writes not null message fields to the data writer.
    public func Put(dw: IDataWriter) { }
    public func PutField(dw: TDataWriter, fs: FieldDescriptor) { }
    
    public var ByteSize: Int { get { let sz = _memoized_size; return (sz > 0) ? sz : GetSerializedSize(); } }
    
    public func MergeFrom(data: Message) {
        MessageCopier().Append(self, data);
    }
    
    public func MergeFrom(data: ArrayRef<Byte>)
    {
        if data.count == 0 { return }
        PBStreamReader(data, 0, data.count).Read(self, data.count);
    }
    
    public func MergeFrom(data: String?)
    {
        if let str = data where str != "" {
            JSStreamReader(data, 0, data.count).Read(self)
        }
    }
    
    //-- TODO: public void WriteTo(System.IO.Stream os, DataEncoding encoding)
    
    public func ToByteArray() -> ArrayRef<Byte> {
        let bytes = self.GetSerializedSize()
        let buffer = ArrayRef<Byte>(count: bytes, repeatedValue: 0)
         Put(PBStreamWriter(buffer, 0, bytes))
        return buffer
    }
    
    public func ToByteArray(bt: ArrayRef<Byte>, offset: Int, count: Int) -> Void {
        Put(PBStreamWriter(bt, offset, count))
    }
    
    public func ToString() -> String {
        return ToString(false)
    }
    
    public func ToString(decorate: Bool) -> String {
        var dts = DataStorage()
       
        var jsw = JSStreamWriter(dts, decorate)
        defer {
            jsw.Dispose()
        }
    
        jsw.Append(self)
        jsw.Flush()
        return jsw.ToString()
    }
    
    public func Equals(message: Message) -> Bool {
        return false;
    }
    
    func GetMapEntry(map: ArrayRef<MapEntry>, key: Int64, ds: MessageDescriptor?) throws -> MapEntry {
        for kv in map {
            if kv.lk == key { return kv }
            if let ds = ds {
                return MapEntry(ds, key)
            }
            throw DataflowException.KeyNotFoundException(String(key))
        }
    }
    
    func GetMapEntry(map: ArrayRef<MapEntry>, key: String, ds: MessageDescriptor?) throws -> MapEntry {
        for kv in map {
            if kv.sk == key { return kv }
            if let ds = ds {
                return MapEntry(ds, key)
            }
            throw DataflowException.KeyNotFoundException(key)
        }
    }
    
    static func _init_ds_(ds: MessageDescriptor, factory: Message, fs: FieldDescriptor...) -> MessageDescriptor {
        return ds.Init(factory, fs)
    }
    
    static func _map_ds_(key: Int, val: Int, vs: MessageDescriptor) -> MessageDescriptor {
        return MessageDescriptor_30("map", key, val, vs)
    }

    public init() {
        
    }
}

// This is an alias to empty message definition.
public class Nothing : Message
{
    public static let Descriptor: MessageDescriptor = MessageDescriptor_20(name: "Nothing", options: Pbs.iNone, factory: Nothing())
    public override func GetDescriptor()  -> MessageDescriptor { return Nothing.Descriptor }
}

public class FieldDescriptor {
    public let iRepeated = 64, iRequired = 128, iPacked = 256, iMap = 512, iSignFmt = 1024, iBoxed = 2048
    private var _options = 0
    
    public var DataSize: Int { get { return _options >> 16 } set { _options = (_options & 0xFFFF) | newValue << 16 } }
    public var DataType: WireType { get { return WireType(rawValue: (_options & 63))! } } //TODO: not sure about WireType conversion here
    public var IsBoxed: Bool { get { return (_options & iBoxed) != 0 } }
    public var IsPacked: Bool { get { return (_options & iPacked) != 0 } }
    public var IsRepeated: Bool { get { return (_options & iRepeated) != 0 } }
    public var IsRequired: Bool { get { return (_options & iRequired) != 0 } }
    public var IsSignFormat: Bool { get { return (_options & iSignFmt) != 0 } }
    public var StringEncoding: Bool { get { return (Id & 0x7) == Pbs.iString } }
    
    public var Id: Int
    public var Name: String
    public var Pos: Int = 0
    public var MessageType: MessageDescriptor?
    
    public init(name: String, id: Int, os: Int) {
        self.Name = name
        self.Id = id
        self._options = os
    }
    
    public init(name: String, id: Int, os: Int, ds: MessageDescriptor) {
        self.Name = name
        self.Id = id
        self._options = os
        self.MessageType = ds
    }
    
    public init(pbid: Int, iwt: Int, name: String, ds: MessageDescriptor? = nil) {
        var iwt = iwt
        self.Name = name;
        self.MessageType = ds
        let wt = WireType(rawValue: iwt)!
        if wt == WireType.Sint32 || wt == WireType.Sint64 {
            iwt |= iSignFmt
        }
        _options = iwt | iBoxed;
        self.Id = Pbs.GetWireId(pbid, wt: wt)
    }
}

public class EnumFieldDescriptor : FieldDescriptor
{
    public init(name: String, value: Int) {
        super.init(name: name, id: value, os: 0)
    }
}

public class MessageDescriptor {
    private static var _no_fields = ArrayRef<FieldDescriptor>()
    internal var _nameIndex: ArrayRef<FieldDescriptor>?
    internal var _idsIndex = ArrayRef<FieldDescriptor>()
    internal var _fields = ArrayRef<FieldDescriptor>()
    internal var _factory: Message?
    private var _options: Int
    
    public var IsKvMap: Bool { get { return (_options & Pbs.iKvMap) != 0 } }
    public var IsListType: Bool { get { return (_options & Pbs.iList) != 0 } }
    public var IsBoxType: Bool { get { return (_options & Pbs.iBox) != 0 } }
    public var IsGoogleType: Bool { get { return (_options & Pbs.iGoogleType) != 0 } }
    public var Name: String
    public var Fields: ArrayRef<FieldDescriptor> { get { return _fields } }
    public var FieldCount: Int { get { return _fields.count } }
    public var Factory: Message? { get { return _factory; } }
    public var HasOptions: Bool { get { return _options != 0 } }
    
    public init(name: String, options: Int, factory: Message?, fs: ArrayRef<FieldDescriptor>) {
        self._factory = factory
        self._options = options
        self.Name = name
        
        if fs.count == 0 {
            _idsIndex = MessageDescriptor._no_fields
            _fields = MessageDescriptor._no_fields
        }
        var pos = 0
        _fields = fs
        for fi in fs.value {
            fi.Pos = pos
            pos += 1
        }
        RecalcIndex()
    }
    
    public convenience init(name: String, options: Int, factory: Message?)  {
        self.init(name: name, options: options, factory: factory, fs: ArrayRef<FieldDescriptor>())
    }
    
    public func Setup(factory: Message, fs: FieldDescriptor...) throws -> MessageDescriptor {
        if _factory != nil {
            throw DataflowException.GenericException("Messaged already initialized")
        }
        _factory = factory
        if fs.count == 0 {
            self._idsIndex = MessageDescriptor._no_fields
            self._fields = MessageDescriptor._no_fields
        }
        else {
            var pos = 0
            self._fields = ArrayRef<FieldDescriptor>(value: fs)
            for fi in self._fields.value {
                fi.Pos = pos
                pos += 1
            }
            RecalcIndex()
        }
    }
    
    public func AddField(name: String, type: WireType, desc: MessageDescriptor, options: Int = 0) {
        let pos = _fields.count;
        let fs = FieldDescriptor(name: name, id: Pbs.GetWireId(pos+1, wt: type), os: type.rawValue | options, ds: desc)
        fs.Pos = pos
        _fields.append(fs)
        _idsIndex = _fields
    }
    
    public func New() -> Message { return Factory!.New() }
    
    public func Find( id: Int) -> FieldDescriptor? {
        let id = id >> 3
        
        // fast index available
        if _idsIndex.count > 0 { return _idsIndex[id-1] }
        
        // PB indexes are sparse, full search required.
        let fcount = _fields.count
        if fcount < 6 {
            for ds in _fields.value {
                if (ds.Id >> 3) == id { return ds }
            }
        }
        else {
            var r = 0
            var h  = fcount
            while r <= h {
                let i = (r + h) >> 1
                let item = _fields[i]
                let c = (item.Id >> 3) - id
                if c < 0 { r = i + 1 }
                else if c == 0 { return item } else { h = i - 1 }
            }
        }
        
        return nil
    }
    
    public func Find(name: String) -> FieldDescriptor? {
        if _nameIndex == nil
        {
            for fi in _fields.value {
                if fi.Name == name { return fi }
            }
        }
        else {
            var r = 0
            var h = _nameIndex!.count - 1
            while r <= h {
                let i = (r + h) >> 1
                let item = _nameIndex![i]
                if item.Name == name { return item } // MO: original code used string.CompareOrdinal, needs testing
                if item.Name < name {
                    r = i + 1
                }
                else {
                    h = i - 1
                }
            }
            
        }
        return nil
    }
    
    
    public func RecalcIndex() {
        _nameIndex = nil
        if _fields.count < 8 {
            let sorted = self.Fields.value.sort { $0.Name > $1.Name }
            _nameIndex = ArrayRef<FieldDescriptor>(value: sorted)
        }
        if _factory == nil { return }

        
        var maxId = 0;
        for fi in _fields.value
        {
            if fi.Id > maxId { maxId = fi.Id }
        }
        maxId = maxId >> 3
        
        if maxId == FieldCount {
            _idsIndex = _fields
        }
        else if maxId < FieldCount * 2 {
            var indexes = Array<FieldDescriptor?>(count: maxId, repeatedValue: nil)
            
            for fi in _fields.value {
                indexes[(fi.Id >> 3) - 1] = fi
            }
            
            _idsIndex = ArrayRef<FieldDescriptor>(value: indexes.flatMap { $0 })
        }
    }
}

public class EnumDescriptor: MessageDescriptor {
    private var _map: ArrayRef<EnumFieldDescriptor>?
    private var _lowId: Int, _maxId: Int
    
    init(name: String, fs: EnumFieldDescriptor...) {
        super.init(name: name, options: 0, factory: nil, fs: ArrayRef<FieldDescriptor>())
        
        Name = name
        _fields = ArrayRef<FieldDescriptor>(value: fs)
        if fs.count == 0 { return }
        
        var pos = 0
        _lowId = Int.max
        _maxId = Int.min
        for ds in _fields.value {
            if ds.Id > _maxId { _maxId = ds.Id }
            if ds.Id < _lowId { _lowId = ds.Id }
            ds.Pos = pos
            pos += 1
        }
        RecalcIndex()
        if _maxId - _lowId >= FieldCount*2 { return }
        
        var map = Array<EnumFieldDescriptor?>(count: _maxId - _lowId + 1, repeatedValue: nil)
        for ds in fs {
            map[ds.Id - _lowId] = ds
        }
        _map = ArrayRef<EnumFieldDescriptor>(value: map.flatMap { $0 })
    }
    
    convenience init() {
        self.init(name: "")
    }
    
    public func GetById(id: Int) -> FieldDescriptor? {
        if let map = _map {
            if id < _lowId || id > _maxId { return nil }
            return map[id - _lowId]
        }
        for ds in _fields.value {
            if ds.Id == id { return ds }
        }
        return nil
    }
}

public class MessageDescriptor_20: MessageDescriptor {

}

public class MessageDescriptor_30: MessageDescriptor {
    private static let map_key_name = "key"
    private static let map_value_name = "value"
    private static let _key_string = FieldDescriptor(pbid: 1, iwt: WireType.String.rawValue, name: MessageDescriptor_30.map_key_name)
    
    init(name: String, options: Int = Pbs.iNone) {
        Name = name
        _options = options
    }
    
    init(name: String, key: Int, value: Int, valMst: MessageDescriptor? = nil) {
        let fs = ArrayRef<FieldDescriptor>()
        
        if key == WireType.String.rawValue {
            fs.append(MessageDescriptor_30._key_string)
        }
        else {
            fs.append(FieldDescriptor(pbid: 1, iwt: key, name: MessageDescriptor_30.map_key_name))
            fs.append(FieldDescriptor(pbid: 2, iwt: value, name: MessageDescriptor_30.map_value_name, ds: valMst))
        }
        
        super.init(name: name, options: Pbs.iKvMap, factory: nil, fs: fs)
        
        
        // base(name, Pbs.iKvMap, null, 
        // key == (int)WireType.String ? _key_string : new FieldDescriptor(1, key, map_key_name), new FieldDescriptor(2, value, map_value_name, valMst)) { }
    }
    
}


public protocol IDataReader
{
    // value/wire types deserializers.
    func AsBit32() throws -> Int32
    func AsBit64() throws -> Int64
    func AsBool() throws -> Bool
    func AsBytes() throws -> ByteArray?
    func AsChar() throws -> UTF16Char
    func AsCurrency() throws -> Currency
    func AsDouble() throws -> Double
    func AsEnum(es: EnumDescriptor) throws -> Int
    func AsInt() throws -> Int32
    func AsLong() throws -> Int64
    func AsString() throws -> String
    func AsFloat() throws -> Float
    // special methods are needed due to Protocol Buffers signed int format optimizations.
    func AsSi32() throws -> Int32
    func AsSi64() throws -> Int64
    // message types deserializer.
    func AsMessage(msg: Message, fs: FieldDescriptor) throws
}

public class TDataReader {
    // value/wire types deserializers.
    func AsBit32() -> Int { preconditionFailure(DataflowException.MustOverride) }
    func AsBit64() -> Int64 { preconditionFailure(DataflowException.MustOverride) }
    func AsBool() -> Bool { preconditionFailure(DataflowException.MustOverride) }
    func AsBytes() -> [Byte] { preconditionFailure(DataflowException.MustOverride) }
    func AsChar() -> UTF16Char { preconditionFailure(DataflowException.MustOverride) }
    func AsCurrency() -> Currency { preconditionFailure(DataflowException.MustOverride) }
    func AsDouble() -> Double { preconditionFailure(DataflowException.MustOverride) }
    func AsEnum(es: EnumDescriptor) -> Int { preconditionFailure(DataflowException.MustOverride) }
    func AsInt() -> Int { preconditionFailure(DataflowException.MustOverride) }
    func AsLong() -> Int64 { preconditionFailure(DataflowException.MustOverride) }
    func AsString() -> String { preconditionFailure(DataflowException.MustOverride) }
    func AsFloat() -> Float { preconditionFailure(DataflowException.MustOverride) }
    // special methods are needed due to Protocol Buffers signed int format optimizations.
    func AsSi32() -> Int32 { preconditionFailure(DataflowException.MustOverride) }
    func AsSi64() -> Int64 { preconditionFailure(DataflowException.MustOverride) }
    // message types deserializer.
    func AsMessage(msg: Message, fs:FieldDescriptor) { preconditionFailure(DataflowException.MustOverride) }
}

public protocol IDataWriter
{
    // value types serializers.
    func AsBytes(fs: FieldDescriptor, bt: [Byte])
    func AsCurrency(fs: FieldDescriptor, cy: Currency)
    func AsDouble(fs: FieldDescriptor, d: Double)
    func AsInt(fs: FieldDescriptor, i: Int32)
    func AsLong(fs: FieldDescriptor, l: Int64)
    func AsString(fs: FieldDescriptor, s: String)
    func AsBool(fs: FieldDescriptor, b: Bool)
    func AsChar(fs: FieldDescriptor, ch: UTF16Char)
    func AsBit32(fs: FieldDescriptor, i: Int32)
    func AsBit64(fs: FieldDescriptor, l: Int64)
    func AsEnum(fs: FieldDescriptor, en: Int64)
    func AsFloat(fs: FieldDescriptor, f: Float)
    func AsSi32(fs: FieldDescriptor, i: Int32)
    func AsSi64(fs: FieldDescriptor, l: Int64)
    // repeated fields serializer, "expands" inside based on field data type.
    func AsRepeated(fs: FieldDescriptor, data: [AnyObject])
    // embedded messages serializer.
    func AsMessage(fs: FieldDescriptor, msg: Message)
}

public class TDataWriter {
    // value types serializers.
    func AsBytes(fs: FieldDescriptor, bt: [Byte]) { preconditionFailure(DataflowException.MustOverride) }
    func AsCurrency(fs: FieldDescriptor, cy: Currency) { preconditionFailure(DataflowException.MustOverride) }
    func AsDouble(fs: FieldDescriptor, d: Double) { preconditionFailure(DataflowException.MustOverride) }
    func AsInt(fs: FieldDescriptor, i: Int32) { preconditionFailure(DataflowException.MustOverride) }
    func AsLong(fs: FieldDescriptor, l: Int64) { preconditionFailure(DataflowException.MustOverride) }
    func AsString(fs: FieldDescriptor, s: String) { preconditionFailure(DataflowException.MustOverride) }
    func AsBool(fs: FieldDescriptor, b: Bool) { preconditionFailure(DataflowException.MustOverride) }
    func AsChar(fs: FieldDescriptor, ch: UTF16Char) { preconditionFailure(DataflowException.MustOverride) }
    func AsBit32(fs: FieldDescriptor, i: Int32) { preconditionFailure(DataflowException.MustOverride) }
    func AsBit64(fs: FieldDescriptor, l: Int64) { preconditionFailure(DataflowException.MustOverride) }
    func AsEnum(fs: FieldDescriptor, en: Int64) { preconditionFailure(DataflowException.MustOverride) }
    func AsFloat(fs: FieldDescriptor, f: Float) { preconditionFailure(DataflowException.MustOverride) }
    func AsSi32(fs: FieldDescriptor, i: Int32) { preconditionFailure(DataflowException.MustOverride) }
    func AsSi64(fs: FieldDescriptor, l: Int64) { preconditionFailure(DataflowException.MustOverride) }
    // repeated fields serializer, "expands" inside based on field data type.
    func AsRepeated(fs: FieldDescriptor, data: [AnyObject]) { preconditionFailure(DataflowException.MustOverride) }
    // embedded messages serializer.
    func AsMessage(fs: FieldDescriptor, msg: Message) { preconditionFailure(DataflowException.MustOverride) }
}

// Base classes for PB/JSON/... (de)serializers.
//public class MessageSerializer {
//    var _storage: StorageWriter
//    
//    init(bts: ArrayRef<Byte>, pos: Int, count: Int) {
//        _storage = StorageWriter(bts, pos: pos, count: count)
//    }
//    
//    init(dts: DataStorage, estimate: Int) {
//        _storage = StorageWriter(dts, estimate: estimate)
//    }
//    
//    public func AppendMessage(msg: Message, ci: MessageDescriptor) {
//        preconditionFailure(DataflowException.MustOverride)
//    }
//}






















