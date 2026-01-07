#ifndef UTILS_H
#define UTILS_H

#include <sstream>
#include <climits>
#include <string>
#include <vector>


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


String removeExt(String n, int s){
	while(s--){
		n.pop_back();
	}	
	return n;
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
}

String floatToStr(float f){
    int caster = f;
    if(f == caster) return intToStr(caster);
    String temp =  (String)std::to_string(f);
    while(temp[temp.size() - 1] == '0') temp.pop_back();
    return temp;
}

#endif // UTILS_H
