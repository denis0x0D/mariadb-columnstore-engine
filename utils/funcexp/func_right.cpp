/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/****************************************************************************
* $Id: func_right.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <my_global.h>
#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
#include "utils_utf8.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "collation.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_right::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}


std::string Func_right::getStrVal(rowgroup::Row& row,
                                  FunctionParm& fp,
                                  bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& type)
{
    CHARSET_INFO* cs = type.getCharset();
    // The original string
    const string& src = fp[0]->data()->getStrVal(row, isNull);
    if (isNull)
        return "";
    if (src.empty() || src.length() == 0)
        return src;
    // binLen represents the number of bytes in src
    size_t binLen = src.length();
    const char* pos = src.c_str();
    const char* end = pos + binLen;

    size_t trimLength = fp[1]->data()->getUintVal(row, isNull);
    if (isNull || trimLength <= 0)
        return "";

    size_t start = cs->numchars(pos, end); // Here, start is number of characters in src
    if (start <= trimLength)
        return src;
    start = cs->charpos(pos, end, start - trimLength); // Here, start becomes number of bytes into src to start copying

    std::string ret(pos+start, binLen-start);
    return ret;
}

} // namespace funcexp
// vim:ts=4 sw=4:

