#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/env.h"

#include <sys/time.h>

unsigned int g_num_4Ks = 0;

struct ctrlr_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct ctrlr_entry	*next;
	char			name[1024];
};

struct ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct ns_entry		*next;
	struct spdk_nvme_qpair	*qpair;
};

typedef struct {
        void *value;
        uint32_t length;
        int32_t offset;
} kv_value;

static struct ctrlr_entry *g_controllers = NULL;
static struct ns_entry *g_namespaces = NULL;

static void
register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns)
{
	struct ns_entry *entry;
	const struct spdk_nvme_ctrlr_data *cdata;

	cdata = spdk_nvme_ctrlr_get_data(ctrlr);

	if (!spdk_nvme_ns_is_active(ns)) {
		printf("Controller %-20.20s (%-20.20s): Skipping inactive NS %u\n",
		       cdata->mn, cdata->sn,
		       spdk_nvme_ns_get_id(ns));
		return;
	}

	entry = malloc(sizeof(struct ns_entry));
	if (entry == NULL) {
		perror("ns_entry malloc");
		exit(1);
	}

	entry->ctrlr = ctrlr;
	entry->ns = ns;
	entry->next = g_namespaces;
	g_namespaces = entry;

	printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
}

struct hello_world_sequence {
	int		lba_offset;
	unsigned int	is_completed;
	struct ns_entry *ns_entry;
	kv_value	**value;
	int		num_lbas;
};

static void
write_io_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	int rc = -EINVAL;
	struct hello_world_sequence	*sequence = arg;

	rc = spdk_nvme_ns_cmd_write(sequence->ns_entry->ns, sequence->ns_entry->qpair, sequence->value[sequence->is_completed]->value,
			sequence->lba_offset, /* LBA start */
			sequence->num_lbas, /* number of LBAs */
			write_io_complete, sequence, 0);
	if (rc != 0) {
		fprintf(stderr, "starting write I/O failed\n");
		exit(1);
	}

	sequence->lba_offset += sequence->num_lbas;
	sequence->is_completed++;
}

static void
read_io_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	int rc = -EINVAL;
	struct hello_world_sequence	*sequence = arg;

	rc = spdk_nvme_ns_cmd_read(sequence->ns_entry->ns, sequence->ns_entry->qpair, sequence->value[0]->value,
			sequence->lba_offset, /* LBA start */
			sequence->num_lbas, /* number of LBAs */
			read_io_complete, sequence, 0);
	if (rc != 0) {
		fprintf(stderr, "starting write I/O failed\n");
		exit(1);
	}

	sequence->lba_offset += sequence->num_lbas;
	sequence->is_completed++;
}

static void show_elapsed_time(struct timeval* start, struct timeval* end, char* msg, int repeat_count){
        long secs_used;
        long micros_used;
        printf("%s start: %lds, %ldus\n", (msg) ? msg : "", start->tv_sec, start->tv_usec);
        printf("%s end: %lds, %ldus\n",(msg) ? msg : "", end->tv_sec, end->tv_usec);
        secs_used=(end->tv_sec - start->tv_sec); //avoid overflow by subtracting first
        micros_used= ((secs_used*1000000) + end->tv_usec) - (start->tv_usec);
        printf("%s elapsed: %ldus %.3fms\n", (msg) ? msg : "", micros_used, ((float)micros_used) / 1000);
	if(repeat_count) {
		printf("==================================================\n");
		printf("%s elapsed: %.3fus per operation\n", (msg) ? msg : "", ((float)micros_used) / repeat_count);
		printf("==================================================\n");
	}
}

static void
write_test(struct ns_entry *ns_entry, kv_value **value)
{
	struct hello_world_sequence	sequence;
	int				rc;
	unsigned int			lbas_per_4K = 0, sector_size = 0;

	struct timeval start;
	struct timeval end;

	sector_size = spdk_nvme_ns_get_sector_size(ns_entry->ns);
	lbas_per_4K = 0x1000 / sector_size;

	printf("%s: Num of 4Ks: %d\n", __func__, g_num_4Ks);

	sequence.is_completed = 0;
	sequence.lba_offset = 0;
	sequence.ns_entry = ns_entry;
	sequence.value = value;
	sequence.num_lbas = lbas_per_4K;

	gettimeofday(&start, NULL);

	rc = spdk_nvme_ns_cmd_write(ns_entry->ns, ns_entry->qpair, value[0]->value,
			0, /* LBA start */
			lbas_per_4K, /* number of LBAs */
			write_io_complete, &sequence, 0);
	if (rc != 0) {
		fprintf(stderr, "starting write I/O failed\n");
		exit(1);
	}

	while(sequence.is_completed != g_num_4Ks) {
		spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
	}

	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Write Test", g_num_4Ks);
}

static void
read_test(struct ns_entry *ns_entry, kv_value **value)
{
	struct hello_world_sequence	sequence;
	int				rc;
	unsigned int			lbas_per_4K = 0, sector_size = 0;

	struct timeval start;
	struct timeval end;

	sector_size = spdk_nvme_ns_get_sector_size(ns_entry->ns);
	lbas_per_4K = 0x1000 / sector_size;

	printf("%s: Num of 4Ks: %d\n", __func__, g_num_4Ks);

	sequence.is_completed = 0;
	sequence.lba_offset = 0;
	sequence.ns_entry = ns_entry;
	sequence.value = value;
	sequence.num_lbas = lbas_per_4K;

	gettimeofday(&start, NULL);

	rc = spdk_nvme_ns_cmd_read(ns_entry->ns, ns_entry->qpair, value[0]->value,
			0, /* LBA start */
			lbas_per_4K, /* number of LBAs */
			read_io_complete, &sequence, 0);
	if (rc != 0) {
		fprintf(stderr, "starting write I/O failed\n");
		exit(1);
	}

	while(sequence.is_completed != g_num_4Ks) {
		spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
	}

	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Read Test", g_num_4Ks);
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Attaching to %s\n", trid->traddr);

	return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	int nsid, num_ns;
	struct ctrlr_entry *entry;
	struct spdk_nvme_ns *ns;
	const struct spdk_nvme_ctrlr_data *cdata = spdk_nvme_ctrlr_get_data(ctrlr);

	entry = malloc(sizeof(struct ctrlr_entry));
	if (entry == NULL) {
		perror("ctrlr_entry malloc");
		exit(1);
	}

	printf("Attached to %s\n", trid->traddr);

	snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	entry->ctrlr = ctrlr;
	entry->next = g_controllers;
	g_controllers = entry;

	num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
	printf("Using controller %s with %d namespaces.\n", entry->name, num_ns);
	for (nsid = 1; nsid <= num_ns; nsid++) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		register_ns(ctrlr, ns);
	}
}

static void
cleanup(void)
{
	struct ns_entry *ns_entry = g_namespaces;
	struct ctrlr_entry *ctrlr_entry = g_controllers;

	while (ns_entry) {
		struct ns_entry *next = ns_entry->next;
		free(ns_entry);
		ns_entry = next;
	}

	while (ctrlr_entry) {
		struct ctrlr_entry *next = ctrlr_entry->next;

		spdk_nvme_detach(ctrlr_entry->ctrlr);
		free(ctrlr_entry);
		ctrlr_entry = next;
	}
}

static void usage(char *program_name)
{
	printf("%s options", program_name);
	printf("\n");
	printf("\t[-n Number of LBAs to do Read / Write]\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;

	while ((op = getopt(argc, argv, "n:")) != -1) {
		switch (op) {
		case 'n':
			g_num_4Ks = atoi(optarg);
			break;
		default:
			usage(argv[0]);
			return 1;
		}
	}

	if(g_num_4Ks == 0)
		g_num_4Ks = 100000;

	return 0;
}

int main(int argc, char **argv)
{
	int rc;
	struct spdk_env_opts opts;
	struct ns_entry *ns_entry = NULL;

	rc = parse_args(argc, argv);
	if (rc != 0) {
		return rc;
	}

	spdk_env_opts_init(&opts);
	opts.name = "hello_world";
	spdk_env_init(&opts);

	printf("Initializing NVMe Controllers\n");

	rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
	if (rc != 0) {
		fprintf(stderr, "spdk_nvme_probe() failed\n");
		cleanup();
		return 1;
	}

	printf("Initialization complete.\n");
	
	ns_entry = g_namespaces;
	ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, 0);
	if (ns_entry->qpair == NULL) {
		printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		return -ENOMEM;
	}

	kv_value** value = (kv_value**)malloc(sizeof(kv_value*)*g_num_4Ks);

	for(unsigned int i = 0; i < g_num_4Ks; i++) {
		value[i] = (kv_value*)malloc(sizeof(kv_value));

		value[i]->value = spdk_zmalloc(4096, 0, NULL);
		value[i]->length = 4096;
		value[i]->offset = 0;

		memset(value[i]->value, 'a'+(i % 26), 4096);
	}

	write_test(ns_entry, value);
	read_test(ns_entry, value);

	for(unsigned int i = 0; i < g_num_4Ks; i++) {
		spdk_free(value[i]->value);
		free(value[i]);
	}

	free(value);

	spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);

	cleanup();
	return 0;
}
