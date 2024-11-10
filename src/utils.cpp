#pragma once

#include <sstream>
#include <string>
#include <vector>




std::vector<std::string> strSplit(const std::string& str, char delimiter) {
    std::stringstream ss(str);
    std::vector<std::string> pieces;
    std::string tmp;
    while (std::getline(ss, tmp, delimiter)) {
        pieces.push_back(tmp);

    }
    return (pieces);
}



bool areDigits(std::string& nums){
    int start = 0;
    if(nums.size() > 0 && nums[0] == '-') start = 1;
    bool are_digits = true;
    for(size_t i = start; i < nums.size(); i++){
        if(nums[i] - '0'  > 9 || nums[i] - '0' < 0) {
            are_digits = false;
        }
    }
    return (are_digits && nums.size() > 0);
}

int str_to_int(std::string& s){
    int start = 0;
    if(s.size() > 0 && s[0] == '-') start = 1;
    if(!areDigits(s)) return 0;
    for(int i = start; i < s.size(); i++) 
        if (s[i] > '9' || s[i] < '0') 
            return 0;
    return (s[0] == '-' ? -stoi(s) : stoi(s));
}

std::string compareStr(std::string& a, std::string& b, std::string& cmp){
    if(areDigits(a) && areDigits(b)){
        if(cmp == ">" && str_to_int(a) > str_to_int(b)) return "1";
        else if(cmp == ">") return "0";

        if(cmp == "<" && str_to_int(a) < str_to_int(b)) return "1";
        else if(cmp == "<") return "0";

        if(cmp == ">=" && str_to_int(a) >= str_to_int(b)) return "1";
        else if(cmp == ">=") return "0";

        if(cmp == "<=" && str_to_int(a) <= str_to_int(b)) {
            return "1";
        }
        else if(cmp == "<=") return "0";
    }
    return "0";
}


std::string removeExt(std::string n, int s){
	while(s--){
		n.pop_back();
	}	
	return n;
}

int strToInt(std::string str){
    std::istringstream iss(str);
	int tmp;
	iss >> tmp;
	return tmp;
}

std::string doubleToStr(double d){
    std::ostringstream ss;
    ss << d;
    std::string s(ss.str());
    return s;
}

std::string floatToStr(float f){
    std::ostringstream ss;
    ss << f;
    std::string s(ss.str());
    return s;
}

std::string intToStr(int t){
    if(t == 0) return "0";
    std::string str = "";
    if(t < 0)  {
        str = "-";
        t *= -1;
    }
    std::vector<char> v;
    while(t > 0){
        v.push_back((t%10) + '0');
        t /= 10;
    }
    for(int i = v.size()-1; i >= 0; i--)
        str+= v[i];

	return str;
}

