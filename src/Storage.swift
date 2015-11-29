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
    
    init() {
        _cb = SegmentCache.instance.GetChars()
        _pos = 0
        _indent = 0
    }
    
    public func ToString() -> String {
        return String(_cb)
    }
}


public class SegmentCache {
    public static var instance = SegmentCache()
    public init() {
        // TODO: implement this class
    }
    
    public func GetChars() -> ArrayRef<Character> {
        return ArrayRef<Character>(count: 8192, repeatedValue: "\0")
    }
    
    public func GetBytes() -> ArrayRef<Byte> {
        return ArrayRef<Byte>(count: 8192, repeatedValue: 0)
    }
}
