//
//  Utils.swift
//  protobuf
//
//  Created by Mikhail Opletayev on 6/4/15.
//  Copyright (c) 2015 Mikhail Opletayev. All rights reserved.
//

import Foundation


extension String {
    
    subscript (i: Int) -> Character {
        return self[self.startIndex.advancedBy(i)]
    }
    
    subscript (i: Int) -> Int8 {
        return Int8(self.utf8[self.utf8.startIndex.advancedBy(i)])
    }
    
    subscript (i: Int) -> String {
        return String(self[i] as Character)
    }
    
    subscript (r: Range<Int>) -> String {
        return substringWithRange(startIndex.advancedBy(r.startIndex)..<startIndex.advancedBy(r.endIndex))
    }
    
    func AsBytes() -> [Byte] {
        var bytes = [Byte]()
        bytes += self.utf8
        return bytes
    }
}

public typealias Byte = UInt8

struct Unsafe {

    static func toByteArray<T>(value: T) -> [Byte] {
        var v = value
        return withUnsafePointer(&v) {
            Array(UnsafeBufferPointer(start: UnsafePointer<Byte>($0), count: sizeof(T)))
        }
    }

    static func fromByteArray<T>(value: [Byte], _: T.Type) -> T {
        return value.withUnsafeBufferPointer {
            return UnsafePointer<T>($0.baseAddress).memory
        }
    }

    static func toInt64Bits<T>(value: T) -> UInt64 {
        var v = value
        return withUnsafePointer(&v) {
            return UnsafePointer<UInt64>($0).memory
        }
    }

    static func fromInt64Bits<T>(bits: UInt64, _: T.Type) -> T {
        var v = bits
        return withUnsafePointer(&v) {
            return UnsafePointer<T>($0).memory
        }
    }
    
    static func toInt32Bits<T>(value: T) -> UInt32 {
        var v = value
        return withUnsafePointer(&v) {
            return UnsafePointer<UInt32>($0).memory
        }
    }
    
    static func fromInt32Bits<T>(bits: UInt32, _: T.Type) -> T {
        var v = bits
        return withUnsafePointer(&v) {
            return UnsafePointer<T>($0).memory
        }
    }
}

public typealias BytePtr = UnsafeMutablePointer<Byte>
public typealias ByteArrayPtr = UnsafeMutableBufferPointer<Byte>

public class ByteArray {
    private let _mem: BytePtr

    let buf: ByteArrayPtr
    let count: Int

    public init(count: Int) {
        self.count = count
        _mem = BytePtr(malloc(self.count))
        buf = ByteArrayPtr(start: _mem, count: self.count)
    }
    
    public convenience init(value: ByteArray) {
        self.init(count: value.count)
        self.blockCopy(value, srcOffset: 0, dstOffset: 0, count: value.count)
    }
    
    public subscript(index: Int) -> Byte {
        get { return buf[index] }
        set(b) { buf[index] = b }
    }
    
    func blockCopy(src: ByteArray, srcOffset: Int, dstOffset: Int, count: Int) {
        // TODO: make sure memcpy is safe to copy onto itself
        memcpy(self.buf.baseAddress + dstOffset, src.buf.baseAddress + srcOffset, count)
    }
    
    deinit {
        free(_mem)
    }
    
    public func toString() -> String {
        let str = NSString(bytes: self.buf.baseAddress, length: self.count, encoding: NSUTF8StringEncoding)
        return str! as String
    }
}

public class ArrayRef<T> {
    public var value: [T] = []
    
    init() {
    }
    
    init(value: [T]) {
        self.value = value
    }
    
    init(count: Int, repeatedValue: T) {
        self.value = Array<T>(count: count, repeatedValue:  repeatedValue)
    }
    
    subscript(index: Int) -> T {
        get { return value[index] }
        set { value[index] = newValue }
    }
    
    var count: Int {
        get { return value.count }
    }
    
    func append(item: T) {
        self.value.append(item)
    }
    
    func blockCopy(srcOffset: Int, dst: ArrayRef<T>, dstOffset: Int, count: Int) {
        dst.value[dstOffset..<(dstOffset+count)] = value[srcOffset..<(srcOffset+count)]
    }
    
    func toString() -> String {
        var str: NSString? = nil
        value.withUnsafeBufferPointer {
            str = NSString(bytes: $0.baseAddress, length: self.count, encoding: NSUTF8StringEncoding)
        }
        return str! as String
    }
}

func blockCopy(src: ByteArrayPtr, srcOffset: Int, dst: ByteArrayPtr, dstOffset: Int, count: Int) {
    memcpy(dst.baseAddress + dstOffset, src.baseAddress + srcOffset, count)
}

func blockCopy(src: UnsafePointer<Byte>, srcOffset: Int, dst: ByteArrayPtr, dstOffset: Int, count: Int) {
    memcpy(dst.baseAddress + dstOffset, src + srcOffset, count)
}