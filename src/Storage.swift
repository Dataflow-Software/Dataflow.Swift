//
//  Storage.swift
//  protobuf
//
//  Created by Mikhail Opletayev on 11/29/15.
//  Copyright © 2015 Mikhail Opletayev. All rights reserved.
//

import Foundation

class SpinLock {
    private var locker = OS_SPINLOCK_INIT
    
    func lock() {
        withUnsafeMutablePointer(&locker, OSSpinLockLock)
    }
    
    func unlock() {
        withUnsafeMutablePointer(&locker, OSSpinLockUnlock)
    }
}

public class TextBuilder {
    private var _cb: ArrayRef<Character>
    private var _pos, _indent: Int
    private var _flush: String = ""
    
    init() {
        _cb = SegmentCache.instance.GetChars()
        _pos = 0
        _indent = 0
    }
    
    deinit {
        SegmentCache.instance.ReturnChars(&_cb)
    }
    
    public func Flush() -> Int {
        if _pos == 0 { return _pos }
        _flush += String(_cb.value[0..<_pos]) // TODO: check if this works correctly
        _pos = 0
        return _pos
    }
    
    public func Append(ch: Character) -> TextBuilder {
        if _pos == _cb.count { Flush() }
        _cb[_pos++] = ch
        return self
    }
    
    public func Append(str: String) -> TextBuilder {
        for ch in str.characters {
            Append(ch)
        }
        return self
    }
    
    public func Append(strings strings: String...) -> TextBuilder {
        for s in strings {
            Append(s)
        }
        return self
    }
    
    public func Append(s: String, var bp: Int, var ep: Int) -> TextBuilder {
        if ep <= bp { return self }
        if ep > s.characters.count { ep = s.characters.count }
        var i = _pos
        while bp < ep {
            if i < _cb.count { _cb[i++] = s[bp++] }
            else {
                _pos = i
                i = Flush()
                _cb[i++] = s[bp++]
            }
        }
        _pos = i
        return self
    }
    
    public func Append(i: Int) -> TextBuilder {
        Append(String(i))
        return self
    }
    
    public func Base64(str: String) -> TextBuilder {
        let utf8str = str.dataUsingEncoding(NSUTF8StringEncoding)
        if let base64 = utf8str?.base64EncodedStringWithOptions(NSDataBase64EncodingOptions(rawValue: 0)) {
            Append(base64)
        }
        return self
    }
    
    public func Hex(i: Int) -> TextBuilder {
        let str = String(format:"%2X", i)
        Append(str)
        return self
    }
    
    public func NewLine() -> TextBuilder {
        if _pos > 0 { Append("\r"); Append("\n") }
        for var i = _indent; i > 0; i-- { Append("\t") }
        return self;
    }
    
    public func NewLine(str: String) -> TextBuilder {
        return NewLine().Append(str)
    }
    
    public func Indent() -> TextBuilder { _indent++; return self }
    public func Indent(str: String) -> TextBuilder { NewLine(str); _indent++; return self }
    public func IndentAt(str: String) -> TextBuilder { Append(str); _indent++; return self }
    public func UnIndent() -> TextBuilder { _indent--; return self }
    public func UnIndent(str: String) -> TextBuilder { _indent--; return NewLine(str) }
    public func OpenScope(str: String) -> TextBuilder { NewLine().Append("{"); _indent++; return self }
    public func CloseScope(str: String) -> TextBuilder { _indent--; return NewLine().Append("}") }
    
    public func Separator(str: String, inout insert: Bool) -> TextBuilder {
        if insert { Append(str) }
        else { insert = true }
        return self
    }
    
    public func Pop() throws -> TextBuilder {
        if _pos > 0 { _pos-- }
        else { throw DataflowException.GenericException("Unable to pop") }
        return self
    }
    
    public func Comma() -> TextBuilder { return Append(",") }
    public func Semi() -> TextBuilder { return Append(";") }
    public func Quote() -> TextBuilder { return Append("\"") }
    public func Space() -> TextBuilder { return Append(" ") }
    public func Nop() -> TextBuilder { return self }
    
    public func ToString() -> String {
        Flush()
        return _flush
    }
}

public struct BitSet64 {
    private var _bits: UInt64
    private let _offset: Int
    
    public init(ps: Int...) throws {
        _bits = 0
        var min = Int.max
        var max = 0
        for i in ps {
            if min > i { min = i }
            if max < i { max = i }
        }
        if max - min > 63 { throw DataflowException.OutOfRangeException }
        _offset = min
        for i in ps { Set(i) }
    }
    
    public func Has(var i: Int) -> Bool { i -= self._offset; return i >= 0 && (_bits & UInt64(1) << UInt64(i)) != 0; }
    public mutating func Set(i: Int) -> Void { self._bits |= UInt64(1) << UInt64(i - self._offset) }
    public subscript(i: Int) -> Bool { return Has(i) }
}


public struct BitSet128
{
    private var _bits, _bit2: UInt64;
    private let _offset: Int;
    
    public init(ps: Int...) throws
    {
        _bits = 0
        _bit2 = 0
        var min = Int.max
        var max = 0
        for i in ps {
            if min > i { min = i }
            if max < i { max = i }
        }
        if max - min > 127  { throw DataflowException.OutOfRangeException }
    
        _offset = min
        for i in ps { Set(i) }
    }
    
    public func Has(var i: Int) -> Bool
    {
        if i < _offset { return false }
        i -= _offset
        return i < 64 ? (_bits & UInt64(1) << UInt64(i)) != 0 : (_bit2 & UInt64(1) << UInt64(i - 64)) != 0
    }
    
    
    public mutating func Set(var i: Int)
    {
        if i < _offset { return }
        i -= _offset
        if i < 64 { self._bits |= UInt64(1) << UInt64(i) }
        else { self._bit2 |= UInt64(1) << UInt64(i - 64) }
    }
    
    public subscript(i: Int) -> Bool { return Has(i) }
}

public class StorageReader {
    private var _db: ArrayRef<Byte>
    private var _bp, _cp, _epos: Int
    private var _limit, _bsize : Int
    private var _dts: DataStorage?;
    
    var Limit: Int {
        get {
            return _limit
        }
        set {
            _limit = newValue
        }
    }
    
    var Position: Int {
        get {
            return _cp - _bp
        }
    }
    
    public init(bt: ArrayRef<Byte>, offset: Int, count: Int) {
        _db = bt;
        _cp = offset
        _bsize =  _cp + count
        _epos = _bsize
        _limit = 0
        _bp = 0
    }
    
    public init(ds: DataStorage) {
        _dts = ds
        _epos = 32
        _cp = 0
        _db = _dts!.Peek(&_epos, cp: &_cp)
        _bsize = _epos
        _bp = _cp
        _epos += _bp
        _limit = 0
    }
    
    public func Reset(ds: DataStorage) {
        _dts = ds
        _epos = 32
        _cp = 0
        _db = _dts!.Peek(&_epos, cp: &_cp)
        _bsize = _epos
        _bp = _cp
        _epos += _bp
    }
    
    public func IsAvailable(bytes: Int) throws -> Bool {
        let sz = _epos - _cp
        if sz >= bytes { return true }
        if let dts = _dts {
            return dts.ContentSize - (_cp - _bp) >= bytes
        }
        throw DataflowException.GenericException("Not enough data")
    }
    
    internal func LoadData(sz: Int) -> Int {
        var lz = _epos - _cp
        if lz > sz { return lz }
        if let dts = _dts {
            if dts.Skip(_cp - _bp) > lz {
                lz = sz < 32 ? 32 : (sz > 8192 ? 8192 : sz)
                _bp = 0
                _db = dts.Peek(&lz, cp: &_bp)
                _cp = _bp
                _epos = _cp + lz
            }
            else { _bp = _cp }
        }
        _bsize = _epos - _cp
        return lz
    }
    
    public func GetData(sz: Int, rqs: Int) throws -> Int {
        let lz = LoadData(sz)
        if lz < rqs { throw DataflowException.GenericException("Cant read enough data") }
        return _cp
    }
    
    public func GetBytes(var bt: ArrayRef<Byte>?, inout btSize: Int) throws -> ArrayRef<Byte> {
        var sz = _epos - _cp
        var lz = btSize
        if lz <= _bsize {
            if lz > sz { try GetData(lz, rqs: lz) }
            if bt == nil {
                btSize = _cp
                bt = _db
            }
            else {
                // TODO this needs to be validated that it works
                // same as Buffer.BlockCopy(_db, _cp, bt, 0, lz);
                let eidx = _cp + lz
                let bytes = _db.value[_cp..<eidx]
                bt!.value.removeAll()
                bt!.value += bytes
            }
            _cp += lz
            _limit -= lz
            return bt!
        }
        if bt == nil { bt = ArrayRef<Byte>() }
        
        if let bt = bt {
            // TODO this needs to be validated that it works
            // same as Buffer.BlockCopy(_db, _cp, bt, 0, sz);
            let eidx = _cp + sz
            let bytes = _db.value[_cp..<eidx]
            bt.value += bytes
            var pos = sz
            for _cp += sz, lz -= sz; lz > 0; _cp += sz {
                let np = try GetData(lz, rqs: 1)
                sz = _epos - np
                if sz > lz { sz = lz }
                // TODO this needs to be validated that it works
                // same as Buffer.BlockCopy(_db, _cp, bt, pos, sz);
                let eidx = _cp + sz
                let bytes = _db.value[_cp..<eidx]
                if bt.count > pos { bt.value.removeRange(pos..<bt.value.count) }
                bt.value += bytes
                lz -= sz
                pos += sz
            }
            _limit -= bt.count
        }
        return bt!
    }
    
//    public byte[] GetBytes(byte[] bt, ref int btSize)
//        {
//            int sz = _epos - _cp, lz = btSize;
//            btSize = 0;
//            if (lz <= _bsize)
//            {
//                if (lz > sz) GetData(lz, lz);
//                if (bt == null) { btSize = _cp; bt = _db; }
//                else Buffer.BlockCopy(_db, _cp, bt, 0, lz);
//                _cp += lz; _limit -= lz;
//                return bt;
//            }
//            if (bt == null) bt = new byte[lz];
//            Buffer.BlockCopy(_db, _cp, bt, 0, sz);
//            var pos = sz;
//            for (_cp += sz, lz -= sz; lz > 0; _cp += sz)
//            {
//                var np = GetData(lz, 1);
//                sz = _epos - np;
//                if (sz > lz) sz = lz;
//                Buffer.BlockCopy(_db, _cp, bt, pos, sz);
//                lz -= sz; pos += sz;
//            }
//            _limit -= bt.Length;
//            return bt;
//        }


}

public class DataStorage {
    public var ContentSize: Int = 0
    public init() {
        
    }
    
    public func Peek(inout epos: Int, inout cp: Int) -> ArrayRef<Byte> {
        return ArrayRef<Byte>()
    }
    
    public func Skip(val: Int) -> Int {
        return 0
    }
}


public class SegmentCache {
    public static var instance = SegmentCache()
    public static let emptyChars = ArrayRef<Character>()
    public static let emptyBytes = ArrayRef<Byte>()
    
    public init() {
        // TODO: implement this class
    }
    
    public func GetChars() -> ArrayRef<Character> {
        return ArrayRef<Character>(count: 8192, repeatedValue: "\0")
    }
    
    public func GetBytes() -> ArrayRef<Byte> {
        return ArrayRef<Byte>(count: 8192, repeatedValue: 0)
    }
    
    public func ReturnChars(inout chars: ArrayRef<Character>) {
        chars = SegmentCache.emptyChars
    }
    
    public func ReturnBytes(inout bytes: ArrayRef<Byte>) {
        bytes = SegmentCache.emptyBytes
    }
}
