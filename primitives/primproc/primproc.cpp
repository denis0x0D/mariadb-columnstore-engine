/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2022 MariaDB Corporation

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

/***********************************************************************
 *   $Id: primproc.cpp 2147 2013-08-14 20:44:44Z bwilkinson $
 *
 *
 ***********************************************************************/
#include <unistd.h>
#include <string>
#include <iostream>
#ifdef QSIZE_DEBUG
#include <iomanip>
#include <fstream>
#endif
#include <csignal>
#include <sys/time.h>
#include <sys/resource.h>
#include <tr1/unordered_set>

#include <clocale>
#include <iterator>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;

#include "messageids.h"
using namespace logging;

#include "primproc.h"
#include "primitiveserver.h"
#include "MonitorProcMem.h"
#include "pp_logger.h"
#include "umsocketselector.h"
#include "serviceprimproc.h"

using namespace primitiveprocessor;

#include "liboamcpp.h"
using namespace oam;

#include "IDBPolicy.h"
using namespace idbdatafile;

#include "cgroupconfigurator.h"

#include "crashtrace.h"
#include "installdir.h"

#include "mariadb_my_sys.h"

#include "spinlock.h"
#include "service.h"
#include "serviceexemgr.h"

int main(int argc, char** argv)
{
  Opt opt(argc, argv);

  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");
  // This is unset due to the way we start it
  program_invocation_short_name = const_cast<char*>("PrimProc");
  // Initialize the charset library
  MY_INIT(argv[0]);

  ServicePrimProc::instance()->setOpt(opt);
  return ServicePrimProc::instance()->Run();
}
