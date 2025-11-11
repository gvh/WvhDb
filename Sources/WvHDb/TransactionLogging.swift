//
//  TransactionLogging.swift
//  WvHDb
//
//  Created by Gardner von Holt on 11/11/25.
//

import Foundation

public protocol TransactionLogging {
    func logUpdateBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data)
    func logUpdateAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data)
    func logInsertAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data)
    func logDeleteBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data)
    func logDeleteBeforeMissing(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String)
}
