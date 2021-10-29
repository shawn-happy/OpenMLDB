/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef SRC_BASE_DDL_PARSER_H_
#define SRC_BASE_DDL_PARSER_H_

#include <map>
#include <string>
#include <vector>

#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"

namespace hybridse::vm {
class RunSession;
class PhysicalOpNode;
}

namespace openmldb::base {

using IndexMap = std::map<std::string, std::vector<::openmldb::common::ColumnKey>>;

class DDLParser {
 public:
    static IndexMap ExtractIndexes(const std::string& sql, const ::hybridse::type::Database& db);

    template <typename T>
    static IndexMap ExtractIndexes(const std::string& sql, const T& schemas);

    static IndexMap ExtractIndexesForBatch(const std::string& sql, const ::hybridse::type::Database& db);

    static std::string Explain(const std::string& sql, const ::hybridse::type::Database& db);

 private:
    // tables are in one db, and db name will be rewritten for simplicity
    static IndexMap ExtractIndexes(const std::string& sql, const hybridse::type::Database& db,
                                   hybridse::vm::RunSession* session);

    // DLR
    static IndexMap ParseIndexes(hybridse::vm::PhysicalOpNode* node);

    static bool GetPlan(const std::string& sql, const hybridse::type::Database& db,
                        hybridse::vm::RunSession* session);

    template <typename T>
    static void AddTables(const T& schema, hybridse::type::Database* db);
};
}  // namespace openmldb::base

#endif  // SRC_BASE_DDL_PARSER_H_
