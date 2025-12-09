#ifndef UTILS_H
#define UTILS_H

#include <sstream>
#include <climits>
#include <string>
#include <vector>

std::pair<String, String> split_scoped_field(String& field) {
    String table = "";
    String col = "";
    bool parsing_table = true;
    for(int i = 0; i < field.size(); ++i){
        if(field[i] == '.') {
            parsing_table = false;
            continue;
        }
        if(parsing_table) table += field[i];
        else col += field[i];
    }
    if(parsing_table) // this field is not scoped.
        return {col, table};
    else 
        return {table, col};
}

String str_toupper(String s){
    int diff = 'A' - 'a';
    String tmp = s;
    for(int i = 0; i < s.size(); ++i)
        if(tmp[i] >= 'a' && tmp[i] <= 'z') tmp[i] += diff;
    return tmp;
}



Vector<String> strSplit(const String& str, char delimiter) {
    std::stringstream ss(str.c_str());
    Vector<String> pieces;
    String tmp;
    while (std::getline(ss, tmp, delimiter)) {
        pieces.push_back(tmp);

    }
    return (pieces);
}



// if floating_sign is not NULL check for digits and return the index of the '.' symbol, 
// if no '.' symbol is found floating_sign will be assigned to -1.
// if floating_sign is NULL or not provided check for digits only.
bool areDigits(String& nums, int* floating_sign = nullptr){
    int start = 0;
    if(nums.size() > 0 && nums[0] == '-') start = 1;
    bool are_digits = true;
    for(size_t i = start; i < nums.size(); i++){
        if(floating_sign != nullptr && nums[i] == '.'){
          *floating_sign = i;
          continue;
        }
        if(nums[i] - '0'  > 9 || nums[i] - '0' < 0) {
            are_digits = false;
        }
    }
    return (are_digits && nums.size() > 0);
}

long long str_to_ll(String& s) {
    return strtoll(s.c_str(), NULL, 10);
}
/*
long long str_to_ll(String& s){
    return strtoll(s.c_str(), NULL, 10);
    int start = 0;
    if(s.size() > 0 && s[0] == '-') start = 1;
    if(!areDigits(s)) return 0;
    for(int i = start; i < s.size(); i++) 
        if (s[i] > '9' || s[i] < '0') 
            return 0;
    return (s[0] == '-' ? -stoi(s) : stoi(s));
}*/

float str_to_float(String& s) {
    return strtof(s.c_str(), NULL);
}
double str_to_double(String& s) {
    return strtod(s.c_str(), NULL);
}
/*
float str_to_float(String& s){
    int start = 0;
    if(s.size() > 0 && s[0] == '-') start = 1;
    int floating_sign_idx = -1;
    if(!areDigits(s, &floating_sign_idx)) return 0;
    for(int i = start; i < s.size(); ++i) {
        if(s[i] == '.' && floating_sign_idx == i) continue;
        if (s[i] > '9' || s[i] < '0') 
            return 0;
    }
    return (s[0] == '-' ? -(stof(s)) : stof(s));
}*/

String removeExt(String n, int s){
	while(s--){
		n.pop_back();
	}	
	return n;
}

int strToInt(String str){
    std::istringstream iss(str.c_str());
	int tmp;
	iss >> tmp;
	return tmp;
}


String intToStr(long long t){
    if(t == 0) return "0";
    String str = "";
    if(t < 0)  {
        str = "-";
        t *= -1;
    }
    Vector<char> v;
    while(t > 0){
        v.push_back((t%10) + '0');
        t /= 10;
    }
    for(int i = v.size()-1; i >= 0; i--)
        str+= v[i];

	return str;
}

String doubleToStr(double d){
    long long caster = d;
    if(d == caster) return intToStr(caster);
    String temp =  (String)std::to_string(d);
    while(temp[temp.size() - 1] == '0') temp.pop_back();
    return temp;
    /*
    std::ostringstream ss;
    ss << d;
    String s(ss.str());
    return s;*/
}

String floatToStr(float f){
    int caster = f;
    if(f == caster) return intToStr(caster);
    String temp =  (String)std::to_string(f);
    while(temp[temp.size() - 1] == '0') temp.pop_back();
    return temp;
    /*
    std::ostringstream ss;
    ss << std::fixed;
    ss << f;
    String s(ss.str());
    return s;*/
}

#endif // UTILS_H
