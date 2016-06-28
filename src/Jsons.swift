//
//  Jsons.swift
//  protobuf
//
//  Created by Mikhail Opletayev on 6/28/16.
//  Copyright © 2016 Mikhail Opletayev. All rights reserved.
//

import Foundation

internal enum JsonToken: Int32 {
    case None = 0
    case Comma = 1
    case Semicolon = 2
    case ArrayEnd = 3
    case ObjectEnd = 4
    case ObjectStart = 5
    case ArrayStart = 6
    case String = 7
    case Number = 8
    case Int = 9
    case True = 10
    case False = 11
    case Null = 12
}

//public class JsonDataReader: IDataReader {
//    private let iCharBufSz = 128
//    
//    
//}