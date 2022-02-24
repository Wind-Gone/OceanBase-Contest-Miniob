//
// Created by Hu on 2021/10/19.
//

#ifndef __OBSERVER_STORAGE_COMMON_DATE_H__
#define __OBSERVER_STORAGE_COMMON_DATE_H__
#include "storage/common/table_meta.h"

enum Calendar { // 月份枚举
    JAN = 1, FEB, MAR, APR,
    MAY, JUN, JUL, AUG,
    SEP, OCT, NOV, DEC,
};

class Date
{
private:
    int year_;
    int month_;
    int day_;

public:
    Date(int year, int month, int day);
    Date();
    ~Date();
    RC init(void *data);
    RC readAndValidate(const char *data);
    void writeToChars(char* data);
    int compare(const Date &b)
    {
        if (this->year_ != b.year_)
            return this->year_ - b.year_;
        else if (this->month_ != b.month_)
            return this->month_ - b.month_;
        else
            return this->day_ - b.day_;
    }
};

#endif // __OBSERVER_STORAGE_COMMON_TABLE_H__
