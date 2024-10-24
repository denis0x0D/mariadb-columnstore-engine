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

#include <iostream>
#include <string>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sstream>
using namespace std;

namespace
{
struct memInfo
{
  size_t mtotal;
  size_t mfree;
};

memInfo getMemInfo()
{
  size_t memTot;
  size_t memFree;

#if   defined(__FreeBSD__)
  string cmd("sysctl -a | awk '/realmem/ {print int(($2+1023)/1024);} /Free Memory Pages/ {print $NF;}'");
  FILE* cmdPipe;
  char input[80];
  cmdPipe = popen(cmd.c_str(), "r");
  input[0] = '\0';
  fgets(input, 80, cmdPipe);
  input[79] = '\0';
  memFree = atoi(input);
  input[0] = '\0';
  fgets(input, 80, cmdPipe);
  input[79] = '\0';
  memTot = atoi(input);
  pclose(cmdPipe);
#else
  ifstream in("/proc/meminfo");
  string input;
  string x;
  getline(in, input);
  {
    stringstream ss;
    ss << input;
    ss >> x;
    ss >> memTot;
  }
  getline(in, input);
  {
    stringstream ss;
    ss << input;
    ss >> x;
    ss >> memFree;
  }
#endif

  // memTot is now in KB, convert to MB
  memTot /= 1024;
  memFree /= 1024;

  memInfo mi;
  mi.mtotal = memTot;
  mi.mfree = memFree;

  return mi;
}

size_t memTotal()
{
  return getMemInfo().mtotal;
}

size_t memFree()
{
  return getMemInfo().mfree;
}

void usage()
{
  cout << "usage: idbmeminfo [-t|f] [-g|m|h]" << endl
       << "\t-t display total system memory (default)" << endl
       << "\t-f display available system memory" << endl
       << "\t-m display memory in MB (default)" << endl
       << "\t-g display memory in GB" << endl
       << "\t-h display this help" << endl;
}

}  // namespace

int main(int argc, char** argv)
{
  opterr = 0;
  int c;
  bool gFlg = false;
  bool tFlg = true;

  while ((c = getopt(argc, argv, "tfgmh")) != EOF)
    switch (c)
    {
      case 'm': gFlg = false; break;

      case 'g': gFlg = true; break;

      case 't': tFlg = true; break;

      case 'f': tFlg = false; break;

      case 'h':
      case '?':
      default:
        usage();
        return (c == 'h' ? 0 : 1);
        break;
    }

  size_t mi;

  if (tFlg)
    mi = memTotal();
  else
    mi = memFree();

  if (gFlg)
  {
    if (mi <= 500)
      mi = 0;
    else
      mi = (mi + 1023) / 1024;
  }

  cout << mi << endl;

  return 0;
}
