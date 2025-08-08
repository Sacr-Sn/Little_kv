#pragma once
#include <vector>
#include <string>
#include <xxhash.h>
#include <iostream>

using namespace std;

class BloomFilter {
public:
    /**
     * ��ʼ����n=Ԥ�ڼ�����, p=������
     * n : Ԥ��Ҫ�洢��key��������ÿ��sst�У���Ҳ����¡��������Ҫ�����Ԫ��������Ԥ��
     * p : �ɽ��ܵ������ʣ������ԣ�����Χ����0 < p < 1������0.01
    */
    BloomFilter(size_t n, double p = 0.01);
    
    // ��Ӽ�
    void add(const std::string& key);
    
    // �����Ƿ���ڣ��������У�
    bool contains(const std::string& key) const;
    
    // ����λ�����С��bytes��
    size_t size() const { return m_bits.size(); }

private:
    size_t m_size;          // λ�����С��bits��
    size_t m_num_hashes;    // ��ϣ��������
    std::vector<uint8_t> m_bits; // λ���飨���ֽڴ洢��

    // �����ϣֵ��ʹ�� xxHash��
    uint64_t hash(const std::string& key, uint32_t seed) const;
};