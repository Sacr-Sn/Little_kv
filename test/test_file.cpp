#include<gtest/gtest.h>
#include"../src/kv_on_file/headers/Little_kv.h"

TEST(FileTest, Put) {
    string path = "../../src/kv_on_file/data/db.txt";
    Little_kv kv(path);
    EXPECT_EQ(kv.get("a"), "one");
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}