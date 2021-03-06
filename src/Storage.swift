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
    
    func under(action: () -> ()) {
        lock()
        action()
        unlock()
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
    
    public func Has(i: Int) -> Bool { var i = i; i -= self._offset; return i >= 0 && (_bits & UInt64(1) << UInt64(i)) != 0; }
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
    
    public func Has(i: Int) -> Bool
    {
        var i = i
        if i < _offset { return false }
        i -= _offset
        return i < 64 ? (_bits & UInt64(1) << UInt64(i)) != 0 : (_bit2 & UInt64(1) << UInt64(i - 64)) != 0
    }
    
    
    public mutating func Set(i: Int)
    {
        var i = i
        if i < _offset { return }
        i -= _offset
        if i < 64 { self._bits |= UInt64(1) << UInt64(i) }
        else { self._bit2 |= UInt64(1) << UInt64(i - 64) }
    }
    
    public subscript(i: Int) -> Bool { return Has(i) }
}

public class StorageReader {
    private var _db: ByteArray
    private var _bp, _cp, _epos: Int
    private var _limit, _bsize : Int
    private var _dts: DataStorage?
    
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
    
    public init(bt: ByteArray, offset: Int, count: Int) {
        _db = bt
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
        _db = ds.Peek(&_epos, peekOffset: &_cp)!
        _bsize = _epos
        _bp = _cp
        _epos += _bp
        _limit = 0
    }
    
    public func Reset(ds: DataStorage) {
        _dts = ds
        _epos = 32
        _cp = 0
        _db = ds.Peek(&_epos, peekOffset: &_cp)!
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
                _db = dts.Peek(&lz, peekOffset: &_bp)!
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
    
    // return continuos block of data, creating new byte[] if necessary.
    public func GetBytes(bt: ByteArray?, inout btSize: Int) throws -> ByteArray {
        var bt = bt
        
        var sz = _epos - _cp
        var lz = btSize
        if lz <= _bsize {
            if lz > sz { try GetData(lz, rqs: lz) }
            if bt == nil {
                btSize = _cp
                bt = _db
            }
            else {
                bt!.blockCopy(_db, srcOffset: _cp, dstOffset: 0, count: sz)
            }
            _cp = _cp + lz
            _limit = _limit - lz
            return bt!
        }
        
        if bt == nil { bt = ByteArray(count: lz) }
        if let bt = bt {
            bt.blockCopy(_db, srcOffset: _cp, dstOffset: 0, count: sz)
            var pos = sz
            _cp = _cp + sz
            lz = lz - sz
            while lz > 0 {
                let np = try GetData(lz, rqs: 1)
                sz = _epos - np
                if sz > lz { sz = lz }
                bt.blockCopy(_db, srcOffset: _cp, dstOffset: pos, count: sz)
                lz = lz - sz
                pos = pos + sz
                _cp = _cp + sz
            }
            _limit -= bt.count
        }
        return bt!
    }
    
    
    public func GetChar() -> UTF16Char {
        var i = self._cp
        while i < self._epos {
            var c = UTF16Char(self._db[i])
            if c > 0x7F {
                if c < 0xE0 {
                    if i + 1 >= _epos { break }
                    i = i + 1
                    c = ((c & 0x1F) << 6) | (UTF16Char(_db[i]) & 0x3F)
                } else {
                    if i + 2 >= _epos { break }
                    i = i + 1
                    c = ((c & 0x0F) << 12) | ((UTF16Char(_db[i]) & 0x3F) << 6)
                    i = i + 1
                    c = c | ((UTF16Char(_db[i]) & 0x3F) << 6)
                }
            }
            self._cp = i + 1
            return UTF16Char(c)
        }
        if LoadData(1) == 0 { return 0 }
        return GetChar()
    }
    
    public func GetByte() throws -> Byte {
        if self._epos - self._cp < 1 { self._cp = try GetData(1, rqs: 1) }
        let bt = _db[_cp]
        self._limit = self._limit - 1
        self._cp = self._cp + 1
        return bt
    }
    
    public func GetB32() throws -> UInt32 {
        if self._epos - self._cp < 4 { self._cp = try GetData(4, rqs: 4) }
        var ui = UInt32(_db[_cp])
        ui = ui + UInt32(_db[_cp + 1]) << 8
        ui = ui + UInt32(_db[_cp + 2]) << 16
        ui = ui + UInt32(_db[_cp + 3]) << 24
        _limit = _limit - 4
        _cp = _cp + 4
        return ui
    }
    
    public func GetB32BE() throws -> UInt32 {
        if self._epos - self._cp < 4 { self._cp = try GetData(4, rqs: 4) }
        var ui = UInt32(_db[_cp + 3])
        ui = ui + UInt32(_db[_cp + 2]) << 8
        ui = ui + UInt32(_db[_cp + 1]) << 16
        ui = ui + UInt32(_db[_cp]) << 24
        _limit = _limit - 4
        _cp = _cp + 4
        return ui
    }
    
    public func GetB64() throws -> Int64 {
        let lo = try self.GetB32()
        let hi = try self.GetB32()
        return (Int64(hi) << 32) + Int64(lo)
    }
    
    public func GetB64BE() throws -> UInt64 {
        let hi = try GetB32BE()
        let lo = try GetB32BE()
        return (UInt64(hi) << 32) + UInt64(lo)
    }
    
    public func UndoIntPB(i: Int32) {
        let sz = Pbs.i32(i)
        _limit = _limit + sz
        _cp = _cp - sz;
    }
    
    public func GetIntPB() throws -> Int32 {
        var i = _cp
        if _epos - _cp < 10 { i = try GetData(5, rqs: 1) }
        var b0 = UInt32(_db[i])
        if b0 < 0x80
        {
            _limit = _limit - 1
            _cp = i + 1;
            return Int32(b0)
        }
        repeat {
            i = i + 1
            var b1 = UInt32(_db[i])
            b0 = (b0 & 0x7F) | (b1 << 7)
            if b1 < 0x80 { break }
            i = i + 1
            b1 = UInt32(_db[i])
            b0 = (b0 & 0x3FFF) | (b1 << 14)
            if b1 < 0x80 { break }
            i = i + 1
            b1 = UInt32(_db[i])
            b0 = (b0 & 0x1FFFFF) | (b1  << 21)
            if b1 < 0x80 { break }
            i = i + 1
            b1 = UInt32(_db[i])
            b0 = (b0 & 0xFFFFFFF) | (b1 << 28)
            if b1 >= 0x80
            {
                for _ in [0..<4] {
                    i = i + 1
                    if _db[i] != 0xFF { throw DataflowException.SerializationException("bad varint") }
                }
                i = i + 1
                if _db[i] != 0x01 { throw DataflowException.SerializationException("bad varint") }
            }
            
        
        } while false
        i = i + 1
        _limit = _limit - (i - _cp)
        _cp = i
        return Int32(b0)
    }
    
    public func GetLongPB() throws -> Int64 {
        var i = _cp
        if _epos - _cp < 10 { i = try GetData(10, rqs: 1) }
        var b0 = Int32(_db[i])
        i = i + 1
        if b0 < 0x80 {
            _limit = _limit - 1
            _cp = i
            return Int64(b0)
        }
        i = i + 1
        b0 = (b0 & 0x7F) | (Int32(_db[i]) << 7)
        if b0 < 0x4000 {
            _limit = _limit - 2
            _cp = i
            return Int64(b0)
        }
        let tval = try Pbs.VarInt64Ex(_db, pos: &i, b0: UInt32(b0))
        _limit = _limit - i - _cp
        _cp = i
        return Int64(tval)
    }
    
     // single byte look-ahead
    public func PeekByte() -> Byte {
        if _epos <= _cp && LoadData(1) == 0 { return 0 }
        return _db[_cp];
    }
    
    // moves current position in the data stream forward
    public func Skip(i: Int) {
        _limit = _limit - i
        let sz = _epos - _cp
        // fast skip inside the buffer.
        if i <= sz {
            _cp = _cp +  i
            return
        }
        if let dts = _dts
        {
            dts.Skip(i + _cp - _bp)
            _cp = 0
            _epos = 0
        }
        else {
            _cp = 0
            _epos = 0
        }
    }
    
    // "returns" unused bytes back to storage
    public func SyncToStorage() {
        let sz = _cp - _bp
        if sz == 0 || _dts == nil { return }
        _bp = _cp
        _dts!.Skip(sz)
    }
}

//-- Base class for message writers that produce byte streams.
public class StorageWriter {
    var _db: ByteArray
    var _cp: Int = 0
    var _bp: Int = 0, _epos: Int = 0
    var dts: DataStorage?
    
    public init(ds: DataStorage, estimate: Int = 0) {
        self.dts = ds
        _cp = 0
        _bp = 0
        _epos = 0
        _db = ByteArray(count: 0) // TODO: otherwise doesn't compile
        Reset(ds, estimate: estimate)
    }
    
    public init(db: ByteArray, pos: Int, count: Int) {
        _db = db
        _bp = pos
        _cp = pos
        _epos = pos + count
        dts = nil
    }
    
    convenience init(reserveSize: Int) {
        self.init(db: ByteArray(count: reserveSize), pos: 0, count: reserveSize)
    }
    
    func Reset(ds: DataStorage?, estimate: Int = 0) {
        self.dts = ds
        if let ds = ds {
            SetSegment(ds.GetNextSegment(estimate))
        }
    }
    
    func SetSegment(ds: DataSegment) {
        _bp = ds.Offset
        _epos = _bp + ds.Size
        _bp = _bp + ds.Count
        _cp = _bp
        _db = ds.Buffer
    }
    
    public func Flush() throws {
        if let dts = dts {
            if _cp == _bp  { return }
            try dts.Commit(_cp - _bp, extend: 0);
            _bp = _cp;
        }
    }
    
    func Flush(xp: Int, sz: Int) throws -> Int {
        return xp + sz <= _epos ? xp : try FlushEx(xp, extraSz: sz);
    }
    
    func FlushEx(endPos: Int, extraSz: Int) throws -> Int {
        if let dts = dts {
            try SetSegment(dts.Commit(endPos - _bp, extend: extraSz))
            return _cp
        } else {
            if endPos < _epos { return endPos }
            throw DataflowException.SerializationException("DataSegment is nil")
        }
    }
    
    public func WriteByte(c: Byte) throws {
        var i = _cp
        if i < _epos { _db[i] = c }
        else
        {
            i = try Flush(i, sz: 1)
            _db[i] = c
        }
        _cp = i + 1
    }
    
    public func WriteByte(b1: Byte, b2: Byte) throws {
        var i = _cp
        if i + 2 > _epos { i = try Flush(i, sz: 2) }
        _db[i + 0] = b1
        _db[i + 1] = b2
        _cp = i + 2
        
    }
    
    public func WriteUTF8Char(c: UTF16Char) throws {
        // TODO: called UTF8 but takes UTF16 as a parameter
        // investigate
        var i = _cp
        // ascii codes shortcut.
        if c < 0x80
        {
            if i >= _epos { i = try Flush(i, sz: 1) }
            _db[i] = UInt8(c)
            _cp = i + 1
            return
        }
        // utf8 expanded codes path.
        if i + 3 > _epos { i = try Flush(i, sz: 3) }
        let bt = _db
        if c < 0x800
        {
            bt[i] = UInt8(0xC0 | (c >> 6))
        }
        else
        {
            bt[i] = UInt8(0xE0 | ((c >> 12) & 0x0F))
            i += 1
            bt[i] = UInt8(0x80 | ((c >> 6) & 0x3F))
        }
        i += 1
        bt[i] = (0x80 | UInt8(c & 0x3F))
        _cp = i + 1
    }
    
    public func WriteB32(v: UInt32) throws {
        var i = _cp
        if i + 4 > _epos { i = try Flush(i, sz: 4) }
        _db[i] = Byte(v)
        _db[i + 1] = Byte(v >> 8)
        _db[i + 2] = Byte(v >> 16)
        _db[i + 3] = Byte(v >> 24)
        _cp = i + 4
    }
    
    public func WriteB32BE(v: UInt32) throws {
        var i = _cp
        if i + 4 > _epos { i = try Flush(i, sz: 4) }
        _db[i + 3] = Byte(v)
        _db[i + 2] = Byte(v >> 8)
        _db[i + 1] = Byte(v >> 16)
        _db[i] = Byte(v >> 24)
        _cp = i + 4
    }
    
    public func WriteB64(v: UInt64) throws {
        var i = _cp
        if i + 8 > _epos { i = try Flush(i, sz: 8) }
        _db[i] = Byte(v);
        _db[i + 1] = Byte(v >> 8)
        _db[i + 2] = Byte(v >> 16)
        _db[i + 3] = Byte(v >> 24)
        _db[i + 4] = Byte(v >> 32)
        _db[i + 5] = Byte(v >> 40)
        _db[i + 6] = Byte(v >> 48)
        _db[i + 7] = Byte(v >> 56)
        _cp = i + 8
    }
    
    public func WriteB64BE(v: UInt64) throws {
        var i = _cp
        if i + 8 > _epos { i = try Flush(i, sz: 8) }
        _db[i + 7] = Byte(v);
        _db[i + 6] = Byte(v >> 8);
        _db[i + 5] = Byte(v >> 16)
        _db[i + 4] = Byte(v >> 24)
        _db[i + 3] = Byte(v >> 32)
        _db[i + 2] = Byte(v >> 40)
        _db[i + 1] = Byte(v >> 48)
        _db[i + 0] = Byte(v >> 56)
        _cp = i + 8
    }
    
    public func WriteBytes(i: Int, bt: ByteArray) throws {
        var pos = 0
        repeat
        {
            let sz = _epos - _cp, lz = i - pos
            if sz >= lz
            {
                _db.blockCopy(bt, srcOffset: pos, dstOffset: _cp, count: lz)
                _cp += lz
                return
            }
            if sz > 0
            {
                _db.blockCopy(bt, srcOffset: pos, dstOffset: _cp, count: sz)
                pos += sz
            }
            _cp = try Flush(_epos, sz: 1)
        } while (true)
    }
    
    public func WriteBytes(bs: ByteArray, bp: Int, cnt: Int) throws {
        var cnt = cnt
        var bp = bp
        var bt = _db
        var i = _cp
        if cnt <= _epos - i
        {
            _cp += cnt
            if (cnt < 12) {
                while (cnt > 0) {
                    bt[i] = bs[bp]
                    i += 1
                    bp += 1
                    cnt -= 1
                }
            }
            else {
                bt.blockCopy(bs, srcOffset: bp, dstOffset: i, count: cnt)
            }
        }
        else
        {
            while (cnt > 0)
            {
                if i >= _epos {
                    i = try Flush(i, sz: 1)
                    bt = _db
                }
                bt[i] = bs[bp]
                i += 1
                bp += 1
                cnt -= 1
            }
            _cp = i
        }
    }
    
    public func Write(bt: ByteArray) throws {
        try WriteBytes(bt, bp: 0, cnt: bt.count)
    }
    
    public func WriteIntPB(i1: Int32, i2: Int32) throws {
        try WriteIntPB(i1)
        try WriteIntPB(i2)
    }
    
    public func WriteIntPB(v: Int32) throws {
        if v >= 0
        {
            var i = _cp
            if i + 5 > _epos { i = try Flush(i, sz: 5) }
            var x = UInt32(v)
            repeat
            {
                if x < 0x80
                {
                    _db[i] = Byte(x);
                    i += 1
                    _cp = i
                    return
                }
                _db[i] = Byte(x | 0x80)
                i += 1
                x = x >> 7
            } while (true)
        }
        try WriteLongPB(Int64(v))
    }
    
    public func WriteLongPB(v: Int64) throws {
        var i = _cp
        if i + 10 > _epos { i = try Flush(i, sz: 10) }
        var x = UInt64(v)
        repeat
        {
            if x < 0x80
            {
                _db[i] = Byte(x)
                i += 1
                _cp = i
                break;
            }
            _db[i] = Byte(x | 0x80);
            i += 1
            x = x >> 7
        } while (true)
    }
    
    public func WriteIntAL(i: Int32) throws {
        
        var i = i
        if i == 0
        {
            try WriteByte(TextUtils.cZero)
            return
        }
        if (i < 0)
        {
            try WriteByte(TextUtils.cDash)
            i = -i
        }
        var sz = 5
        if i < 10000 {
            sz = i < 100 ? (i < 10 ? 1 : 2) : (i < 1000 ? 3 : 4)
        }
        else {
            var x = i / 100000
            while x > 0 {
                sz += 1
                x = x / 10
            }
        }
        var ip = _cp + sz
        if ip > _epos { ip = try Flush(_cp, sz: sz) + sz }
        _cp = ip
        var dx = i
        while i != 0 {
            let bt = Byte(i - dx) + TextUtils.cZero
            dx /= 10
            ip -= 1
            _db[ip] = bt
            i = dx
        }
    }
    
    public func WriteIntAL8(i: Int32) throws {
        var i = i
        var ip = _cp + 8
        if ip > _epos { ip = try Flush(_cp, sz: 8) + 8 }
        var dx = i
        while i != 0 {
            let bt = Byte(i - dx) + TextUtils.cZero;
            dx /= 10
            ip -= 1
            _db[ip] = bt
            i = dx
        }
        ip -= 1
        while ip >= _cp {
             _db[ip] = TextUtils.cZero
            ip -= 1
        }
        _cp += 8;
    }
    
    public func WriteLongAL(i: Int64, quote_longs: Bool = true) throws {
        var i = i
        // for JSON encoding anything above 2^52 must be string encoded to preserve precision on roundtrip.
        let num_bits: Int64 = 52, billion: Int64 = 1000000000
        var sign = false
        if i < 0
        {
            sign = true
            i = -i
        }
        
        if (i >> 31) != 0
        {
            if sign { try WriteByte(TextUtils.cDash) }
            try WriteIntAL(Int32(i));
        }
        else
        {
            if quote_longs && (i >> num_bits) != 0 { try WriteByte(TextUtils.cDblQuote) }
            if sign { try WriteByte(TextUtils.cDash) }
            
            var i2 = i / billion
            i -= i2 * billion
            if i2 < billion {
                try WriteIntAL(Int32(i2))
            }
            else
            {
                let i3 = i2 / billion
                try WriteIntAL(Int32(i3))
                i2 -= i3 * billion
                try WriteIntAL8(Int32(i2))
            }
            try WriteIntAL8(Int32(i))
            if quote_longs && (i >> num_bits) != 0 {
                try WriteByte(TextUtils.cDblQuote);
            }
        }
    }
    
    public func WriteString(s: String) throws {
        var i = _cp - 1, ep = _epos - 3
        var bt = _db
        for c in s.utf16
        {
            i += 1
            if i >= ep {
                i = try Flush(i, sz: 3)
                bt = _db
            }
            if c < 0x80 { bt[i] = UInt8(c) }
            else
            {
                if c < 0x800 {
                    bt[i] = Byte(0xC0 | (c >> 6))
                }
                else
                {
                    bt[i] = Byte(0xE0 | ((c >> 12) & 0x0F))
                    i += 1
                    bt[i] = Byte(0x80 | ((c >> 6) & 0x3F))
                }
                i += 1
                bt[i] =  Byte(0x80 | (c & 0x3F))
            }
        }
        _cp = i + 1
    }
    
    public func WriteStringUE(s: String) throws {
        try WriteStringA(s)
    }
    
    public func WriteStringA(s: String) throws {
        var i = _cp - 1
        var bt = _db
        for c in s.utf8 {
            i += 1
            if i < _epos {
                bt[i] = c
            }
            else
            {
                i = try Flush(i, sz:1)
                bt = _db
                bt[i] = c;
            }
        }
        _cp = i + 1
    }
    
    public func WriteBase64String(dt: ByteArray, pos: Int, size: Int) throws {
        let bs64 = TextUtils.Base64String
        var bt = _db
        
        var i = _cp, left = size % 3, ep = pos + size - left
        var rp = pos
        while rp < ep {
            if i + 4 > _epos { i = try Flush(i, sz: 4); bt = _db; }
            let b1: Int = Int((dt[rp] << 16) | (dt[rp + 1] << 8) | dt[rp + 2])
            bt[i + 0] = bs64[b1 >> 18]
            bt[i + 1] = bs64[(b1 >> 12) & 63]
            bt[i + 2] = bs64[(b1 >> 6) & 63]
            bt[i + 3] = bs64[b1 & 63]
            i += 4;
            rp += 3
            
        }
        if left > 0 {
            if i + 4 > _epos { i = try Flush(i, sz: 4); bt = _db; }
            bt[i + 3] = TextUtils.cEquals
            var b1: Int = Int(dt[ep])
            bt[i] = bs64[b1 >> 2]
            b1 = (b1 & 3) << 4
            if left == 1
            {
                bt[i + 1] = bs64[b1]
                bt[i + 2] = TextUtils.cEquals
            } else {
                let b2: Int = Int(dt[ep + 1])
                bt[i + 1] = bs64[(b2 >> 4) | b1]
                bt[i + 2] = bs64[(b2 & 15) << 2]
            }
            i += 4
        }
        _cp = i
    }
    
    public func ToByteArray() throws -> ByteArray {
        let sz = _cp - _bp
        if sz == 0 { return ByteArray(count: 0) }
        if let dts = dts where dts.ContentSize > 0 {
            try Flush()
            let bts = dts.ToByteArray()
            dts.Reset()
            SetSegment(dts.GetNextSegment(1))
            return bts
        }
        if sz == _db.count { return _db }
        
        let bt = ByteArray(count: sz)
        bt.blockCopy(_db, srcOffset: _bp, dstOffset: 0, count: sz)
        return bt
    }
    
    public func ToString() throws -> String {
        let sz = _cp - _bp
        if sz == 0 { return "" }
        if let dts = dts where dts.ContentSize > 0 {
            try self.Flush()
            return dts.ToString()
        }
        let str = NSString(bytes: _db._mem, length: sz, encoding: NSUTF8StringEncoding)
        return str as! String
    }
}

public class DataStorage {
    private var _last: DataSegment?
    private var _list: DataSegment?
    private var _cp: Int = 0
    
    public var ContentType: String?
    public var ContentSize: Int = 0
    public var TransmittedCount: Int = 0
    
    public var IsEmpty: Bool { get { return ContentSize == 0 } }
    
    public init() {
    }
    
    public init(size: Int) {
        if size <= SegmentCache.instance.iBlockSize {
            _list = SegmentCache.instance.Get()
        } else {
            _list = DataSegment(size: size)
        }
        _last = _list
    }
    
    deinit {
        _list?.Release(true)
        _list = nil
        _last = nil
    }
    
    func Append(data: ByteArray) {
        var sz = data.count
        if sz == 0 { return }
        
        ContentSize += sz
        
        // if its large than the page just keep it as its own segment
        if sz > SegmentCache.instance.iBlockSize {
            let ns = DataSegment(buffer: data, offset: 0, size: sz)
            ns.Count = sz
            if let last = _last { last.Next = ns } // TODO: check with Viktor on this, shouldn't it be ns.Next = _last?
            else { _list = ns }
            _last = ns
            return
        }

        // add initial block if needed
        if _last == nil {
            _last = SegmentCache.instance.Get()
            _list = _last
        }
        guard let last = _last else {
            return
        }
        
        var bz = last.FreeSpace
        if bz > sz {
            bz = sz
            sz = 0
        }
        else {
            sz -= bz
        }
        last.Buffer.blockCopy(data, srcOffset: 0, dstOffset: last.Offset + last.Count, count: bz)
        last.Count += bz
        if sz <= 0 { return }
        
        // copy remainder of the data to new segment
        let ds = SegmentCache.instance.Get()
        last.Next = ds
        _last = ds
        last.Buffer.blockCopy(data, srcOffset: bz, dstOffset: last.Offset, count: sz)
        last.Count = sz
    }
    
    func GetNextSegment(bytes: Int) -> DataSegment {
        if let last = _last where last.FreeSpace >= bytes {
            return last
        }
        let ds = SegmentCache.instance.Get()
        if _last == nil {
            _list = ds
        }
        else {
            _last?.Next = ds
        }
        _last = ds
        return ds
    }
    
    func GetSegmentToRead(size: Int = 0) -> DataSegment {
        return GetNextSegment(size)
    }
    
    func ReadCommit(size: Int) {
        ContentSize += size
        TransmittedCount += size
        _last?.Count += size
    }
    
    func GetSegmentToSend() throws -> DataSegment? {
        if ContentSize == 0 { return nil }
        if _list == nil || _list?.Count == 0 { throw DataflowException.InvalidOperationException }
        return _list
    }
    
    func CommitSentBytes() -> Bool
    {
        if let ds = _list {
            TransmittedCount += ds.Count
            ContentSize -= ds.Count
            ds.Count = 0
            if ds === _last { return true }
            _list = ds.Next
            ds.Release(false)
        }
        return ContentSize == 0
    }
    
    func Commit(count: Int, extend: Int) throws -> DataSegment {
        if let last = _last {
            if count == 0 { return last }
            ContentSize += count
            let cnt = count + last.Count
            if cnt > last.Size { throw DataflowException.InvalidOperationException }
            last.Count = cnt
            if extend <= 0 { return last }
        }
        return GetNextSegment(extend)
    }
    
    func Reset() -> DataStorage {
        if ContentSize == 0 { return self }
        if _list !== _last {
            _list?.Next?.Release(true)
            _list?.Next = nil
            _last = _list
        }
        _cp = 0
        _last?.Count = 0
        ContentSize = 0
        TransmittedCount = 0
        return self
    }
    
    func ToByteArray() -> ByteArray {
        let bts = ByteArray(count: ContentSize)
        if _last === _list {
            if let last = _last where last.Count != 0 {
                bts.blockCopy(last.Buffer, srcOffset: last.Offset, dstOffset: 0, count: bts.count)
                return bts
            }
        }
        // slow path, when data does not fit into single buffer
        if let list = _list {
            var pos = list.Count - _cp;
            bts.blockCopy(list.Buffer, srcOffset: list.Offset, dstOffset: 0, count: pos)
            var ls = list.Next
            while ls != nil {
                if let ls = ls {
                    bts.blockCopy(ls.Buffer, srcOffset: ls.Offset, dstOffset: pos, count: ls.Count)
                    pos = pos + ls.Count
                }
                ls = ls?.Next
            }
        }
        return bts
    }
    
    func ToString() -> String {
        let data = ToByteArray()
        return data.toString()
    }
    
    // removes data region from the storage
    func Cut(size: Int, offset: Int) {
        var offset = offset
        var size = size
        
        // update content size
        if offset + size <= ContentSize { ContentSize -= size }
        else { ContentSize = offset }
        offset = offset + _cp
        
        // check if data block is affected
        var ds = _list
        repeat {
            if let ds = ds {
                let cnt = ds.Count
                if cnt <= offset { offset = offset - cnt }
                else {
                    size = ds.Cut(offset, sz: size)
                    if size == 0 { return }
                    var ns = ds.Next
                    while ns != nil {
                        if ns!.Count > size {
                            ns!.Cut(0, sz: size)
                            return
                        } else {
                            ns!.Release(false)
                            ns = ns!.Next
                            ds.Next = ns
                        }
                    }
                    _last = ds
                }
            }
            
            ds = ds?.Next
        } while(ds != nil)
        
    }
    
    // returns continuos data buffer for the region of data inside storage
    func Peek(inout peekSize: Int, inout peekOffset: Int) -> ByteArray? {
        if _list == nil {
            peekSize = 0
            return nil
        }
        var pos = _cp + peekOffset
        var bx = _list
        repeat {
            var dc = _list!.Count
            if pos >= dc { pos = pos - dc }
            else {
                dc = dc - pos
                pos = pos + bx!.Offset
                if dc >= peekSize || bx!.Next == nil {
                    peekSize = dc
                    peekOffset = pos
                    return bx!.Buffer
                }
                if let nx = bx!.Next {
                    peekSize = min(peekSize, dc + nx.Count)
                    let bt = ByteArray(count: peekSize)
                    bt.blockCopy(bx!.Buffer, srcOffset: pos, dstOffset: 0, count: dc)
                    bt.blockCopy(nx.Buffer, srcOffset: nx.Offset, dstOffset: dc, count: peekSize - dc)
                    peekOffset = 0
                    return bt
                }
            }
            bx = bx?.Next
        } while bx != nil
        peekSize = 0
        return nil
    }
    
    // moves current position in the data stream forward.
    func Skip(skip: Int) -> Int  {
        var skip = skip
        
        self.ContentSize = self.ContentSize - skip
        
        // if skip past eof clear storage.
        if self.ContentSize <= 0 || _list == nil {
            self.Reset()
            return 0
        }
        
        let sz = self._list!.Count - _cp
        if skip < sz {
            self._cp = self._cp + skip
        }
        else {
            skip = skip - sz
            var next = _list!.Next
            repeat {
                
                // if list head is not init block, release it.
                if self._list !== self._last { _list!.Release(false) }
                
                assert(next != nil)  // if next == nil { throw DataflowException.StorageState }
                self._list = next
                
                // if skip lands inside new list head update cp and we are done.
                if skip < next!.Count {
                    self._cp = skip
                    break
                }
                // going to remove next/list block on loop repeat.
                skip = skip - next!.Count
                next = next!.Next

            } while true
        }
        
        return self.ContentSize
    }
}

class DataSegment {
    let _mem: ByteArray
    private let _cache: SegmentCache? = nil
    
    var Next: DataSegment? = nil
    
    var Count: Int = 0
    var Offset: Int = 0
    
    var Size: Int = 0
    var FreeSpace: Int { get { return Size - Count } }
    var Buffer: ByteArray { get { return _mem } }
    
    init(cache: SegmentCache) {
        _mem = ByteArray(count: cache.iBlockSize)
        self.Size = _mem.count
    }
    
    init(size: Int) {
        _mem = ByteArray(count: size)
        self.Size = size
    }
    
    init(buffer: ByteArray, offset: Int, size: Int) {
        _mem =  buffer
        self.Offset = offset
        self.Size = size
    }
    
    func Release(chain: Bool) -> DataSegment? {
        if chain { Next?.Release(chain) }
        if let c = _cache { c.Release(self) }
        Count = 0
        Offset = 0
        return nil
    }
    
    func Cut(pos: Int, sz: Int) -> Int {
        var xz = pos + sz
        if xz >= Count {
            if pos >= Count { return sz }
            else {
                xz -= Count
                Count = pos
                return xz
            }
        }
        
        // TODO: test this
        _mem.blockCopy(_mem, srcOffset: Offset + xz, dstOffset: Offset + pos, count: Count - xz)
        Count -= xz
        return 0
    }
}

public class SegmentCache {
    public var iBlockSize = 0x8000
    public static var instance = SegmentCache()
    
    private var _lock = SpinLock()
    private var _free: DataSegment? = nil
    private var _freeCount: Int = 0
    private var _keepCount: Int = 1
    
    
    public init(keepCount: Int = 1) {
        _keepCount = keepCount
    }
    
    public init(blockSize: Int, keepCount: Int = 64) {
        iBlockSize = blockSize
        _keepCount = keepCount
    }
    
    func Get() -> DataSegment {
        var ds: DataSegment? = nil
        _lock.lock()
        
        if let free = _free {
            ds = free
            _free = free.Next
            _freeCount -= 1
        }
        else {
            ds = DataSegment(cache:  self)
        }
        _lock.unlock()
        return ds!
    }
    
    func Release(ds: DataSegment) {
        _lock.lock()
        if _freeCount < _keepCount {
            ds.Next = _free
            _free = ds
            _freeCount += 1
        }
        _lock.unlock()
    }
}
