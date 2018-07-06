#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "plugin.h"
#include "argconfig.h"

static int version(struct plugin *plugin)
{
	struct program *prog = plugin->parent;

	if (plugin->name)
		printf("%s %s version %s\n", prog->name, plugin->name, prog->version);
	else
		printf("%s version %s\n", prog->name, prog->version);
	return 0;
}

static int help(int argc, char **argv, struct plugin *plugin)
{
	char man[0x100];
	struct program *prog = plugin->parent;

	if (argc == 1) {
		general_help(plugin);
		return 0;
	}

	if (plugin->name)
		sprintf(man, "%s-%s-%s", prog->name, plugin->name, argv[1]);
	else
		sprintf(man, "%s-%s", prog->name, argv[1]);
	if (execlp("man", "man", man, (char *)NULL))
		perror(argv[1]);
	return 0;
}

void usage(struct plugin *plugin)
{
	struct program *prog = plugin->parent;

	if (plugin->name)
		printf("usage: %s %s %s\n", prog->name, plugin->name, prog->usage);
	else
		printf("usage: %s %s\n", prog->name, prog->usage);
}

void general_help(struct plugin *plugin)
{
	struct program *prog = plugin->parent;
//	struct plugin *extension;
//	unsigned i = 0;

	printf("%s-%s\n", prog->name, prog->version);

	usage(plugin);

	printf("\n");
	print_word_wrapped(prog->desc, 0, 0);
	printf("\n");

	if (plugin->desc) {
		printf("\n");
		print_word_wrapped(plugin->desc, 0, 0);
		printf("\n");
	}

	printf("\nThe following are all implemented sub-commands:\n");

//	for (; plugin->commands[i]; i++)
//		printf("  %-*s %s\n", 15, plugin->commands[i]->name,
//					plugin->commands[i]->help);

	printf("    Admin Commands:\n");
	printf("        list            List all NVMe devices and namespaces on machine\n");
	printf("        id-ctrl         Send NVMe Identify Controller\n");
	printf("        list-ctrl       Send NVMe Identify Controller List, display structure\n");
	printf("        get-log         Generic NVMe get log, returns log in raw format\n");
	printf("            ./kv_nvme get-log <device> [--log-id=<log-id> | -i <log-id>]\n");
	printf("                                       [--log-len=<log-len> | -l <log-len>]\n");
	printf("            ex) ./kv_nvme get-log 0000:02:00.0 --log-id=0xc0, --log-len=512    \n");
	printf("        smart-log       Retrieve SMART Log, show it\n");
	printf("        format          Format namespace with new block format\n");
	printf("            ./kv_nvme format <device> [--ses=<ses> | -s <ses>] // (optional) Secure Erase Settings. 1: default, 0: No secure erase \n");
	printf("        admin-passthru  Submit arbitrary admin command, return results\n");
	printf("        io-passthru     Submit an arbitrary IO command, return results\n");
	printf("            ./kv_nvme admin-passthru <device> [--opcode=<opcode> | -o <opcode>]  \n");
        printf("            ./kv_nvme io-passthru    <device> [--flags=<flags> | -f <flags>] [--rsvd=<rsvd> | -R <rsvd>]\n");
        printf("                                              [--namespace-id=<nsid>] [--cdw2=<cdw2>] [--cdw3=<cdw3>]\n");
        printf("                                              [--cdw10=<cdw10>] [--cdw11=<cdw11>] [--cdw12=<cdw12>]\n");
        printf("                                              [--cdw13=<cdw13>] [--cdw14=<cdw14>] [--cdw15=<cdw15>]\n");
        printf("                                              [--data-len=<data-len> | -l <data-len>]\n");
        printf("                                              [--metadata-len=<len> | -m <len>]\n");
        printf("                                              [--input-file=<file> | -f <file>]\n");
        printf("                                              [--read | -r ] [--write | -w]\n");
        printf("                                              [--timeout=<to> | -t <to>]\n");
        printf("                                              [--show-command | --dry-run | -s]\n");
        printf("                                              [--raw-binary=<filepath> | -b <filepath>]\n");
	printf("            ex) ./kv_nvme admin-passthru 0000:02:00.0 --opcode=06 --data-len=4096 --cdw10=1 -r\n");
	printf("            ex) ./kv_nvme io-passthru 0000:02:00.0 --opcode=2 --namespace-id=1 --data-len=4096 --read --cdw10=0 --cdw11=0 --cdw12=0x70000\n");
	printf("        fw-download     Download new firmware then activate the new firmware\n");
	printf("            ./kv_nvme fw-download <device> [--fw=<firmware-file> | -f <firmware-file>]\n");
	printf("                                           [--xfer=<transfer-size> | -x <transfer-size>]\n");
	printf("                                           [--offset=<offset> | -o <offset>]\n");
	printf("            ex) ./kv_nvme fw-download 0000:02:00.0 --fw=EHA50K0F_ENC.bin\n");
	printf("        list-it         List all iterators on a given NVMe device\n");
	printf("            ex) ./kv_nvme list-it 0000:02:00.0\n");
	printf("        reset           Resets the controller\n");
	printf("        version         Shows the program version\n");
	printf("        help            Display this help\n\n");
	printf("    IO Commands:        (currently, only support KV-SSD)\n");
	printf("        write           Submit a write command \n");
	printf("            ./kv_nvme write <device>  [ -k <key>]              // (required), key to be written\n");
	printf("                                      [ -l <key-len>]          // (required), length of the key to be written \n");
	printf("                                      [ -v <value-file>]       // (required) file includes data to be written \n");
	printf("                                      [ -s <value-size>]       // (required) size of data to be written \n");
	printf("                                      [ -o <value-offset>]     // (optional) currently not supported \n");
	printf("                                      [ -i <io-option>]        // (optional) 0: default, 1: idempotent \n");
	printf("                                      [ -n <namespace>]        // identifier of desired namespace (0 or 1), defaults to 0 \n");
	printf("                                      [ -w ]                   // (optional) displaying arguments of the command \n");
	printf("                                      [ -t ]                   // (optional) show latency of the executed command \n");
	printf("                                      [ -a ]                   // (optional) for using asynchronous mode, sync mode is default \n");
	printf("            ex) ./kv_nvme write 0000:02:00.0 -k keyvalue12345678 -l 16 -v value.txt -o 0 -s 4096 -a -w -t   // write (16B key, 4KB value from value.txt) in async mode. \n");
	printf("        read            Submit a read command\n");
	printf("            ./kv_nvme read <device>   [ -k <key>]              // (required), key to be read\n");
	printf("                                      [ -l <key-len>]          // (required), length of the key to be read\n");
	printf("                                      [ -s <value-size>]       // (required) size of data to be read\n");
	printf("                                      [ -v <value-file>]       // (optional) file where the read value to be written, if not set, read value will be written to stdout \n");
	printf("                                      [ -o <value-offset>]     // (optional) currently not supported\n");
	printf("                                      [ -i <io-option>]        // (optional) 0: default, 2: only read value size of the give key (support large-value only)\n");
	printf("                                      [ -n <namespace>]        // identifier of desired namespace (0 or 1), defaults to 0 \n");
	printf("                                      [ -w ]                   // (optional) displaying arguments of the command\n");
	printf("                                      [ -t ]                   // (optional) show latency of the executed command\n");
	printf("                                      [ -a ]                   // (optional) for using asynchronous mode, sync mode is default\n");
	printf("            ex) ./kv_nvme read 0000:02:00.0 -k keyvalue12345678 -l 16 -s 4096 -t -w                         // read (16B key, 4KB value) sync mode\n");
	printf("            ex) ./kv_nvme read 0000:02:00.0 -k keyvalue12345678 -l 16 -s 4096 -v output.txt -t -w           // read (16B key, 4KB value) sync mode, then write read value to output.txt \n");

	printf("        delete          Submit a delete command\n");
	printf("            ./kv_nvme delete <device> [ -k <key>]              // (required), key to be read\n");
	printf("                                      [ -l <key-len>]          // (required), length of the key to be read\n");
	printf("                                      [ -i <io-option>]        // (optional) 0: default\n");
	printf("                                      [ -n <namespace>]        // identifier of desired namespace (0 or 1), defaults to 0 \n");
	printf("                                      [ -w ]                   // (optional) displaying arguments of the command\n");
	printf("                                      [ -t ]                   // (optional) show latency of the executed command\n");
	printf("                                      [ -a ]                   // (optional) for using asynchronous mode, sync mode is default\n");

	printf("        exist           Submit a exist command\n");
        printf("            ./kv_nvme exist <device>  [ -k <key>]              // (required), key to check\n");
        printf("                                      [ -l <key-len>]          // (required), length of the key to be read\n");
        printf("                                      [ -i <io-option>]        // (optional) 0: default\n");
        printf("                                      [ -n <namespace>]        // identifier of desired namespace (0 or 1), defaults to 0 \n");
        printf("                                      [ -w ]                   // (optional) displaying arguments of the command\n");
        printf("                                      [ -t ]                   // (optional) show latency of the executed command\n");
        printf("                                      [ -a ]                   // (optional) for using asynchronous mode, sync mode is default\n");
	printf("            ex) ./kv_nvme exist 0000:02:00.0 -k keyvalue12345678 -l 16 -t -w     // exist command sync mode\n");

	printf("        open-it         Open a new iterator\n");
        printf("            ./kv_nvme open-it <device> [ -p <prefix>]               // prefix of keys to iterate, defaults to 0x0 \n");
        printf("                                       [ -b <bitmask>]              // bitmask of prefix to apply, defaults to 0xffffffff \n");
        printf("                                       [ -n <namespace>]            // identifier of desired namespace (0 or 1), defaults to 0 \n");
        printf("                                       [ -i <iterate_type>]         // type of iterator (1: KEY_ONLY, 2: KEY_WITH_RETRIEVE, 3: KEY_WITH_DELETE) \n");
	printf("            ex) ./kv_nvme open-it 0000:02:00.0 -p 0x30303030 -i 2   // open a new KEY_ONLY iterator whose prefix is \"0000\" (0x30303030) \n");

	printf("        read-it         Reads keys or a key/value pair with the given iterator\n");
        printf("            ./kv_nvme read-it <device> [ -i <iterator-id>]      // (required) identifier of iterator to read \n");
        printf("                                       [ -s <value-size>]       // size of length to iterate read once \n");
	printf("                                       [ -v <value-file>]       // (optional) file where the read value to be written, if not set, read value will be written to stdout \n");
        printf("                                       [ -w ]                   // (optional) displaying arguments of the command\n");
        printf("                                       [ -t ]                   // (optional) show latency of the executed command\n");
        printf("                                       [ -a ]                   // (optional) for using asynchronous mode, sync mode is default\n");
	printf("            ex) ./kv_nvme read-it 0000:02:00.0 -i 1 -w          // issue a iterate read command on iterate_handle (0x1) \n");

	printf("        close-it        Close an opened iterator\n");
        printf("            ./kv_nvme close-it <device> [ -i <iterator-id>]     // (required) identifier of iterator to close \n");
	printf("            ex) ./kv_nvme close-it 0000:02:00.0 -i 1            // Close an iterator whose iterator_id is 0x1 \n");

//	printf("  %-*s %s\n", 15, "version", "Shows the program version");
//	printf("  %-*s %s\n", 15, "help", "Display this help");
	printf("\n");

	if (plugin->name)
		printf("See '%s %s help <command>' for more information on a specific command\n",
			prog->name, plugin->name);
	else
		printf("See '%s help <command>' for more information on a specific command\n",
			prog->name);

	/* The first plugin is the built-in. If we're not showing help for the
	 * built-in, don't show the program's other extensions */
	if (plugin->name)
		return;

//	extension = prog->extensions->next;
//	if (!extension)
//		return;

//	printf("\nThe following are all installed plugin extensions:\n");
//	while (extension) {
//		printf("  %-*s %s\n", 15, extension->name, extension->desc);
//		extension = extension->next;
//	}
//	printf("\nSee '%s <plugin> help' for more information on a plugin\n",
//			prog->name);
}

int handle_plugin(int argc, char **argv, struct plugin *plugin)
{
	unsigned i = 0;
	char *str = argv[0];
	char use[0x100];

	struct plugin *extension;
	struct program *prog = plugin->parent;

	if (!argc) {
		general_help(plugin);
		return 0;
	}

	if (!plugin->name)
		sprintf(use, "%s %s <device> [OPTIONS]", prog->name, str);
	else
		sprintf(use, "%s %s %s <device> [OPTIONS]", prog->name, plugin->name, str);
	argconfig_append_usage(use);

	/* translate --help and --version into commands */
	while (*str == '-')
		str++;

	for (; plugin->commands[i]; i++) {
		struct command *cmd = plugin->commands[i];

		if (!strcmp(str, "help"))
			return help(argc, argv, plugin);
		if (!strcmp(str, "version"))
			return version(plugin);
		if (strcmp(str, cmd->name))
			continue;

		return (cmd->fn(argc, argv, cmd, plugin));
	}

	/* Check extensions only if this is running the built-in plugin */
	if (plugin->name) { 
		printf("ERROR: Invalid sub-command '%s' for plugin %s\n", str, plugin->name);
		return -ENOTTY;
        }

	extension = plugin->next;
	while (extension) {
		if (!strcmp(str, extension->name))
			return handle_plugin(argc - 1, &argv[1], extension);

		/* If the command is executed with the extension name and
		 * command together ("plugin-command"), run the plug in */
		if (!strncmp(str, extension->name, strlen(extension->name))) {
			argv[0] += strlen(extension->name);
			while (*argv[0] == '-')
				argv[0]++;
			return handle_plugin(argc, &argv[0], extension);
		}
		extension = extension->next;
	}
	printf("ERROR: Invalid sub-command '%s'\n", str);
	return -ENOTTY;
}
