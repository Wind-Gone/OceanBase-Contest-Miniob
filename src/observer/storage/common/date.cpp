#include <limits.h>
#include <string.h>
#include <algorithm>

#include "storage/common/table.h"
#include "storage/common/table_meta.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/date.h"

using namespace common;

Date::Date(int year, int month, int day) : year_(year),
                                           month_(month),
                                           day_(day){};

Date::Date() : year_(-1),
               month_(-1),
               day_(-1){};

Date::~Date()
{
}


RC Date::readAndValidate(const char *data)
{
    static std::map<Calendar, int> month_to_day = {
        {Calendar::JAN, 31}, {Calendar::FEB, 28}, {Calendar::MAR, 31},
        {Calendar::APR, 30}, {Calendar::MAY, 31}, {Calendar::JUN, 30},
        {Calendar::JUL, 31}, {Calendar::AUG, 31}, {Calendar::SEP, 30},
        {Calendar::OCT, 31}, {Calendar::NOV, 30}, {Calendar::DEC, 31},
    };
    std::vector<char *> results;
    char *data_cp = strdup(data);
    for (char *p = data_cp; *p != '\0'; p++) {
      if (!isdigit(*p) && *p != '-') {
        // LOG_ERROR("日期格式不能出现除了数字和分隔符以外的字符");
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    }
    split_string(data_cp, '-', results, true);
    if (results.size() != 3)
    {
        // LOG_ERROR("分隔符数量不为2");
        free(data_cp);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    
    long year, month, day;

    if (!str_to_val(std::string(results[0]), year) || !str_to_val(std::string(results[1]), month) || !str_to_val(std::string(results[2]), day))
    {
        free(data_cp);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }

    if (month >= 1 && month <= 12) // 月份合法
    {
        int m_to_d = month_to_day[Calendar(month)];
        if (month == 2 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)) // 闰年二月
            m_to_d += 1;
        if (day < 1 || day > m_to_d) // 日期不合理
        {
            // LOG_ERROR("日期有误");
            free(data_cp);
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
    } else  // 月份不合理
    {
    //   LOG_ERROR("月份有误");
      free(data_cp);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    this->day_ = static_cast<int>(day);
    this->month_ = static_cast<int>(month);
    this->year_ = static_cast<int>(year);
    free(data_cp);
    return RC::SUCCESS;
}

void Date::writeToChars(char* data) {
    sprintf(data, "%d-%02d-%02d", year_, month_, day_);
}