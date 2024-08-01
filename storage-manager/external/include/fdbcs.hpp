#pragma once

#include <bits/stdc++.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <foundationdb/fdb_c.h>

namespace FDBCS {
using ByteArray = std::string;

class Transaction {
public:
  Transaction() = delete;
  Transaction(const Transaction &) = delete;
  Transaction(Transaction &&) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&) = delete;
  explicit Transaction(FDBTransaction *tnx) : tnx_(tnx) {}
  ~Transaction() {
    if (tnx_) {
      fdb_transaction_destroy(tnx_);
      tnx_ = nullptr;
    }
  }

  void set(const ByteArray &key, const ByteArray &value) const {
    if (tnx_) {
      fdb_transaction_set(tnx_, (uint8_t *)key.c_str(), key.length(),
                          (uint8_t *)value.c_str(), value.length());
    }
  }

  std::pair<bool, ByteArray> get(const ByteArray &key) {
    if (tnx_) {
      FDBFuture *future =
          fdb_transaction_get(tnx_, (uint8_t *)key.c_str(), key.length(), 0);
      auto err = fdb_future_block_until_ready(future);
      if (err) {
        std::cerr << "fdb_future_block_until_read error, code: " << (int)err
                  << std::endl;
        return {false, {}};
      }

      const uint8_t *outValue;
      int outValueLength;
      fdb_bool_t present;
      err = fdb_future_get_value(future, &present, &outValue, &outValueLength);
      if (err) {
        std::cerr << "fdb_future_get_value error, code: " << (int)err
                  << std::endl;
        return {false, {}};
      }

      if (present) {
        return {true, ByteArray(outValue, outValue + outValueLength)};
      } else {
        return {false, {}};
      }
    }
    return {false, {}};
  }

  void remove(const ByteArray &key) {
    if (tnx_) {
      fdb_transaction_clear(tnx_, (uint8_t *)key.c_str(), key.length());
    }
  }

  bool commit() {
    if (tnx_) {
      FDBFuture *future = fdb_transaction_commit(tnx_);
      auto err = fdb_future_block_until_ready(future);
      if (err) {
        std::cerr << "fdb_future_block_until_ready error, code: " << (int)err
                  << std::endl;
        return false;
      }
      fdb_future_destroy(future);
    }
    return true;
  }

private:
  FDBTransaction *tnx_{nullptr};
};

class FDBNetwork {
public:
  FDBNetwork() = default;
  FDBNetwork(const FDBNetwork &) = delete;
  FDBNetwork(FDBNetwork &&) = delete;
  FDBNetwork &operator=(const FDBNetwork &) = delete;
  FDBNetwork &operator=(FDBNetwork &&) = delete;

  ~FDBNetwork() {
    auto err = fdb_stop_network();
    if (err)
      std::cerr << "fdb_stop_network error, code: " << err << std::endl;
    netThread.join();
  }

  bool setUpAndRunNetwork() {
    auto err = fdb_setup_network();
    if (err) {
      std::cerr << "fdb_setup_network error, code: " << (int)err << std::endl;
      return false;
    }

    netThread = std::thread([] {
      // TODO: Try more than on time.
      auto err = fdb_run_network();
      if (err) {
        std::cerr << "fdb_run_network error, code: " << (int)err << std::endl;
        abort();
      }
    });
    return true;
  }

private:
  std::thread netThread;
};

class FDBDataBase {
public:
  FDBDataBase() = delete;
  FDBDataBase(const FDBDataBase &) = delete;
  FDBDataBase &operator=(FDBDataBase &) = delete;
  FDBDataBase(FDBDataBase &&) = delete;
  FDBDataBase &operator=(FDBDataBase &&) = delete;
  explicit FDBDataBase(FDBDatabase *database) : database_(database) {}

  ~FDBDataBase() {
    if (database_)
      fdb_database_destroy(database_);
  }

  std::unique_ptr<Transaction> createTransaction() {
    FDBTransaction *tnx;
    auto err = fdb_database_create_transaction(database_, &tnx);
    if (err) {
      std::cerr << "fdb_database_create_transaction error, code: " << (int)err
                << std::endl;
      return nullptr;
    }
    return std::make_unique<Transaction>(tnx);
  }

private:
  FDBDatabase *database_;
};

class DataBaseCreator {
public:
  static std::unique_ptr<FDBDataBase>
  createDataBase(const std::string clusterFilePath) {
    FDBDatabase *database;
    auto err = fdb_create_database(clusterFilePath.c_str(), &database);
    if (err) {
      std::cerr << "fdb_create_database error, code: " << (int)err << std::endl;
      return nullptr;
    }
    return std::make_unique<FDBDataBase>(database);
  }
};

bool setAPIVersion(int apiVersion) {
  return fdb_select_api_version(apiVersion);
}
}  // namespace FDBCS
