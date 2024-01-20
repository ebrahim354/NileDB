#pragma once

#include <sstream>
#include <vector>


std::vector<std::string> strSplit(const std::string &str, char delimiter) {
    std::stringstream ss(str);
    std::vector<std::string> pieces;
    std::string tmp;
    while (std::getline(ss, tmp, delimiter)) {
        pieces.push_back(tmp);

    }
    return (pieces);
}

bool areDigits(std::string nums){
    bool are_digits = true;
    for(size_t i = 0; i < nums.size(); i++){
        if(nums[i] - '0'  > 9 || nums[i] - '0' < 0) {
            are_digits = false;
        }
    }
    return (are_digits && nums.size() > 0);
}

