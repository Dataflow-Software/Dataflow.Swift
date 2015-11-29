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
        return substringWithRange(Range(start: startIndex.advancedBy(r.startIndex), end: startIndex.advancedBy(r.endIndex)))
    }
    
    func AsBytes() -> [Byte] {
        var bytes = [Byte]()
        bytes += self.utf8
        return bytes
    }
}

public typealias Byte = UInt8

struct Unsafe {

    static func toByteArray<T>(var value: T) -> [Byte] {
        return withUnsafePointer(&value) {
            Array(UnsafeBufferPointer(start: UnsafePointer<Byte>($0), count: sizeof(T)))
        }
    }

    static func fromByteArray<T>(value: [Byte], _: T.Type) -> T {
        return value.withUnsafeBufferPointer {
            return UnsafePointer<T>($0.baseAddress).memory
        }
    }

    static func toInt64Bits<T>(var value: T) -> UInt64 {
        return withUnsafePointer(&value) {
            return UnsafePointer<UInt64>($0).memory
        }
    }

    static func fromInt64Bits<T>(var bits: UInt64, _: T.Type) -> T {
        return withUnsafePointer(&bits) {
            return UnsafePointer<T>($0).memory
        }
    }
    
    static func toInt32Bits<T>(var value: T) -> UInt32 {
        return withUnsafePointer(&value) {
            return UnsafePointer<UInt32>($0).memory
        }
    }
    
    static func fromInt32Bits<T>(var bits: UInt32, _: T.Type) -> T {
        return withUnsafePointer(&bits) {
            return UnsafePointer<T>($0).memory
        }
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
}