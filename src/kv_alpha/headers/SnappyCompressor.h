#pragma once

#include <string>

class SnappyCompressor {
public:
    // ѹ��������ԭʼ�ַ��������ѹ���ַ���
    static bool compress(const std::string& input, std::string& output);

    // ��ѹ������ѹ���ַ����������ѹ���
    static bool decompress(const std::string& input, std::string& output);
};
