#include <bits/stdc++.h>
#include "../src/NileDB.cpp"


int main() {
    if(sizeof(void*) != 8) {
        std::cout << "This is not a 64 bit arch, but: " << sizeof(void*)*8 << " bit arch\n";
        return 1;
    }
    auto seed = std::time(nullptr);
    std::cout << "seed: " << seed << "\n";
    std::srand(seed);
    uint64_t mx = (uint64_t)0xFFFFFFFFFFFFFFFF;
    uint64_t rnd = 0;
    int sz = 0;
    uint64_t output = 0;
    uint64_t bytes_decoded = 0;
    bool err = false;
    uint8_t bytes[9];
    int expected_sz = 0;
    for(uint64_t i = 0; i < 1000*1000*250; ++i){
        // fill the bytes array with random values, 
        // to test correct read counts.
        for(int j = 0; j < 9; ++j){
            bytes[j] = std::rand()/256;
        }
        rnd = std::rand();
        uint64_t tmp = rnd;
        expected_sz = 0;
        if(tmp < 1<<7) expected_sz = 1;
        else if(tmp < (uint64_t)1<<14) expected_sz = 2;
        else if(tmp < (uint64_t)1<<21) expected_sz = 3;
        else if(tmp < (uint64_t)1<<28) expected_sz = 4;
        else if(tmp < (uint64_t)1<<35) expected_sz = 5;
        else if(tmp < (uint64_t)1<<42) expected_sz = 6;
        else if(tmp < (uint64_t)1<<49) expected_sz = 7;
        else if(tmp < (uint64_t)1<<56) expected_sz = 8;
        else  expected_sz = 9;
        sz = varint_encode(bytes, rnd);
        bytes_decoded = varint_decode(bytes, &output);
        if(output != rnd || sz != expected_sz || bytes_decoded != expected_sz) {
            std::cout << "expected_sz: " << expected_sz << ", bytes_decoded:" << bytes_decoded
                << ", bytes_encoded: " << sz
                << std::endl;
            std::cout << i << " expected: " << (uint64_t)rnd << ", output: " << (uint64_t)output << "\n";
            err = true;
            break;
        }
    }
    if(!err){
        std::cout  << "DONE!\n" << std::endl;
        return 0;
    }

    for(int i = 0; i < sz; ++i){
        std::cout << (int16_t) bytes[i] << " ";
    }
    std::cout << std::endl;
    std::cout << (uint64_t)bytes_decoded << " bytes, value: " << output << "\n";

    return 1;
}


