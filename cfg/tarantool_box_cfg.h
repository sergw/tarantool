#ifndef tarantool_cfg_CFG_H
#define tarantool_cfg_CFG_H

#include <stdio.h>
#include <sys/types.h>

/*
 * Autogenerated file, do not edit it!
 */

typedef struct tarantool_cfg_namespace_index_key_field {
	unsigned char __confetti_flags;

	int32_t	fieldno;
	char*	type;
} tarantool_cfg_namespace_index_key_field;

typedef struct tarantool_cfg_namespace_index {
	unsigned char __confetti_flags;

	char*	type;
	int32_t	unique;
	tarantool_cfg_namespace_index_key_field**	key_field;
} tarantool_cfg_namespace_index;

typedef struct tarantool_cfg_namespace {
	unsigned char __confetti_flags;

	int32_t	enabled;
	int32_t	cardinality;
	int32_t	estimated_rows;
	tarantool_cfg_namespace_index**	index;
} tarantool_cfg_namespace;

typedef struct tarantool_cfg {
	unsigned char __confetti_flags;


	/* username to switch to */
	char*	username;

	/*
	 * save core on abort/assert
	 * deprecated; use ulimit instead
	 */
	int32_t	coredump;

	/*
	 * admin port
	 * used for admin's connections
	 */
	int32_t	admin_port;

	/* Log verbosity, possible values: ERROR=1, CRIT=2, WARN=3, INFO=4(default), DEBUG=5 */
	int32_t	log_level;

	/* Size of slab arena in GB */
	double	slab_alloc_arena;

	/* Size of minimal allocation unit */
	int32_t	slab_alloc_minimal;

	/* Growth factor, each subsequent unit size is factor * prev unit size */
	double	slab_alloc_factor;

	/* working directory (daemon will chdir(2) to it) */
	char*	work_dir;

	/* name of pid file */
	char*	pid_file;

	/*
	 * logger command will be executed via /bin/sh -c {}
	 * example: 'exec cronolog /var/log/tarantool/%Y-%m/%Y-%m-%d/tarantool.log'
	 * example: 'exec extra/logger.pl /var/log/tarantool/tarantool.log'
	 * when logger is not configured all logging going to STDERR
	 */
	char*	logger;

	/* make logging nonblocking, this potentially can lose some logging data */
	int32_t	logger_nonblock;

	/* delay between loop iterations */
	double	io_collect_interval;

	/* size of listen backlog */
	int32_t	backlog;

	/* network io readahead */
	int32_t	readahead;

	/*
	 * # BOX
	 * Snapshot directory (where snapshots get saved/read)
	 */
	char*	snap_dir;

	/* WAL directory (where WALs get saved/read) */
	char*	wal_dir;

	/* Primary port (where updates are accepted) */
	int32_t	primary_port;

	/* Secondary port (where only selects are accepted) */
	int32_t	secondary_port;

	/* Warn about requests which take longer to process, in seconds. */
	double	too_long_threshold;

	/*
	 * A custom process list (ps) title string, appended after the standard
	 * program title.
	 */
	char*	custom_proc_title;

	/* Memcached emulation is enabled if memcached == 1 */
	int32_t	memcached;

	/* namespace used for memcached emulation */
	int32_t	memcached_namespace;

	/* maximum rows to consider per expire loop iteration */
	int32_t	memcached_expire_per_loop;

	/* tarantool will try to iterate over all rows within this time */
	int32_t	memcached_expire_full_sweep;

	/* Do not write into snapshot faster than snap_io_rate_limit MB/sec */
	double	snap_io_rate_limit;

	/* Write no more rows in WAL */
	int32_t	rows_per_wal;

	/*
	 * fsync WAL delay, only issue fsync if last fsync was wal_fsync_delay
	 * seconds ago.
	 * WARNING: actually, several last requests may stall fsync for much longer
	 */
	int32_t	wal_fsync_delay;

	/* size of WAL writer request buffer */
	int32_t	wal_writer_inbox_size;

	/*
	 * Local hot standby (if enabled, the server will run in local hot standby
	 * mode, continuously fetching WAL records from shared local directory).
	 */
	int32_t	local_hot_standby;

	/*
	 * Delay, in seconds, between successive re-readings of wal_dir.
	 * The re-scan is necessary to discover new WAL files or snapshots.
	 */
	double	wal_dir_rescan_delay;

	/*
	 * Panic if there is an error reading a snapshot or WAL.
	 * By default, panic on any snapshot reading error and ignore errors
	 * when reading WALs.
	 */
	int32_t	panic_on_snap_error;
	int32_t	panic_on_wal_error;

	/*
	 * Remote hot standby (if enabled, the server will run in hot standby mode
	 * continuously fetching WAL records from wal_feeder_ipaddr:wal_feeder_port
	 */
	int32_t	remote_hot_standby;
	char*	wal_feeder_ipaddr;
	int32_t	wal_feeder_port;
	tarantool_cfg_namespace**	namespace;
} tarantool_cfg;

#ifndef CNF_FLAG_STRUCT_NEW
#define CNF_FLAG_STRUCT_NEW	0x01
#endif
#ifndef CNF_FLAG_STRUCT_NOTSET
#define CNF_FLAG_STRUCT_NOTSET	0x02
#endif
#ifndef CNF_STRUCT_DEFINED
#define CNF_STRUCT_DEFINED(s) ((s) != NULL && ((s)->__confetti_flags & CNF_FLAG_STRUCT_NOTSET) == 0)
#endif

void init_tarantool_cfg(tarantool_cfg *c);

int fill_default_tarantool_cfg(tarantool_cfg *c);

void swap_tarantool_cfg(struct tarantool_cfg *c1, struct tarantool_cfg *c2);

void parse_cfg_file_tarantool_cfg(tarantool_cfg *c, FILE *fh, int check_rdonly, int *n_accepted, int *n_skipped);

void parse_cfg_buffer_tarantool_cfg(tarantool_cfg *c, char *buffer, int check_rdonly, int *n_accepted, int *n_skipped);

int check_cfg_tarantool_cfg(tarantool_cfg *c);

int dup_tarantool_cfg(tarantool_cfg *dst, tarantool_cfg *src);

void destroy_tarantool_cfg(tarantool_cfg *c);

char *cmp_tarantool_cfg(tarantool_cfg* c1, tarantool_cfg* c2, int only_check_rdonly);

typedef struct tarantool_cfg_iterator_t tarantool_cfg_iterator_t;
tarantool_cfg_iterator_t* tarantool_cfg_iterator_init();
char* tarantool_cfg_iterator_next(tarantool_cfg_iterator_t* i, tarantool_cfg *c, char **v);

#endif
