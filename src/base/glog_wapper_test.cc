//
// glog_wapper_test.cc
// Copyright 2017 4paradigm.com

#include "base/glog_wapper.h"

#include <iostream>
#include <string>

#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class GlogWapperTest : public ::testing::Test {
 public:
    GlogWapperTest() {}
    ~GlogWapperTest() {}
};

TEST_F(GlogWapperTest, Log) {
    ::rtidb::base::SetLogLevel(DEBUG);
    std::string path = "hello";
    ::rtidb::base::SetLogFile(path);
    PDLOG(INFO, "hello %d %f", 290, 3.1);
    std::string s = "word";
    PDLOG(INFO, "hello %s", s);
    PDLOG(WARNING, "this is a warning %s", "hello");
    DEBUGLOG("hello %d", 233);
    uint64_t time = 123456;
    DEBUGLOG("[Gc4TTL] segment gc with key %lu, consumed %lu, count %lu", time,
             time + 100, time - 100);
}

}  // namespace base
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
