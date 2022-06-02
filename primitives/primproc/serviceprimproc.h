/* Copyright (C) 2022 Mariadb Corporation.

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

#pragma once

#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>
#include <stdexcept>
#include <map>
#include <string>
#include <thread>

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

#include "service.h"
#include "prioritythreadpool.h"
#include "pp_logger.h"

class Opt
{
 public:
  int m_debug;
  bool m_fg;
  Opt(int m_debug, bool m_fg) : m_debug(m_debug), m_fg(m_fg)
  {
  }

  Opt(int argc, char* argv[]) : m_debug(0), m_fg(false)
  {
    int c;

    while ((c = getopt(argc, argv, "df")) != EOF)
    {
      switch (c)
      {
        case 'd': m_debug++; break;
        case 'f': m_fg = true; break;
        case '?':
        default: break;
      }
    }
  }
};

class ServicePrimProc : public Service, public Opt
{
 public:
  static ServicePrimProc* instance();

  void setOpt(const Opt& opt)
  {
    m_debug = opt.m_debug;
    m_fg = opt.m_fg;
  }

  void LogErrno() override
  {
    cerr << strerror(errno) << endl;
  }

  void ParentLogChildMessage(const std::string& str) override
  {
    cout << str << endl;
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
  std::atomic_flag& getStartupRaceFlag()
  {
    return startupRaceFlag_;
  }

 private:
  ServicePrimProc() : Service("PrimProc"), Opt(0, false)
  {
  }

  static ServicePrimProc* fInstance;
  // Since C++20 flag's init value is false.
  std::atomic_flag startupRaceFlag_{false};
  boost::shared_ptr<threadpool::PriorityThreadPool> primServerThreadPool;
  boost::shared_ptr<threadpool::PriorityThreadPool> OOBThreadPool;
};
