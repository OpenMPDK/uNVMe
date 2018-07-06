#ifndef _DISCOVER_H
#define _DISCOVER_H

extern int fdiscover(const char *desc, int argc, char **argv, bool connect);
extern int fconnect(const char *desc, int argc, char **argv);
extern int fdisconnect(const char *desc, int argc, char **argv);

#endif
