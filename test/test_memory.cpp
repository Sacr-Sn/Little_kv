#include<gtest/gtest.h>
#include"../src/kv_on_memory/headers/Little_kv.h"

TEST(Little_kvTest, Put) {
    Little_kv kv;
    EXPECT_EQ(kv.put("a", "123"), true);
}



TEST(Little_kvTest, Del) {
    Little_kv kv;
    kv.put("d", "dv");
    EXPECT_EQ(kv.get("d"), "dv");
    EXPECT_EQ(kv.del("d"), true);
    EXPECT_EQ(kv.get("d"), "null");
    EXPECT_EQ(kv.del("d"), false);
}

TEST(Little_kvTest, Put2) {
    Little_kv kv;
    kv.put("e", "ev");
    EXPECT_EQ(kv.get("e"), "ev");
    EXPECT_EQ(kv.put("e", "ev2"), true);
    EXPECT_EQ(kv.get("e"), "ev2");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}