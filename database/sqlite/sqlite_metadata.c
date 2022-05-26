// SPDX-License-Identifier: GPL-3.0-or-later

#include "sqlite_functions.h"
#include "sqlite_metadata.h"

//const char *metadata_sync_config[] = {
//    "CREATE TABLE IF NOT EXISTS dimension_delete (dimension_id blob, dimension_name text, chart_type_id text, "
//    "dim_id blob, chart_id blob, host_id blob, date_created);",
//
//    "CREATE INDEX IF NOT EXISTS ind_h1 ON dimension_delete (host_id);",
//
//    "CREATE TRIGGER IF NOT EXISTS tr_dim_del AFTER DELETE ON dimension BEGIN INSERT INTO dimension_delete "
//    "(dimension_id, dimension_name, chart_type_id, dim_id, chart_id, host_id, date_created)"
//    " select old.id, old.name, c.type||\".\"||c.id, old.dim_id, old.chart_id, c.host_id, strftime('%s') FROM"
//    " chart c WHERE c.chart_id = old.chart_id; END;",
//
//    "DELETE FROM dimension_delete WHERE host_id NOT IN"
//    " (SELECT host_id FROM host) OR strftime('%s') - date_created > 604800;",
//
//    NULL,
//};

uv_mutex_t metadata_async_lock;

void metadata_database_init_cmd_queue(struct metadata_database_worker_config *wc)
{
    wc->cmd_queue.head = wc->cmd_queue.tail = 0;
    wc->queue_size = 0;
    fatal_assert(0 == uv_cond_init(&wc->cmd_cond));
    fatal_assert(0 == uv_mutex_init(&wc->cmd_mutex));
}

//int metadata_database_enq_cmd_noblock(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
//{
//    unsigned queue_size;
//
//    /* wait for free space in queue */
//    uv_mutex_lock(&wc->cmd_mutex);
//    if ((queue_size = wc->queue_size) == METADATA_DATABASE_CMD_Q_MAX_SIZE || wc->is_shutting_down) {
//        uv_mutex_unlock(&wc->cmd_mutex);
//        return 1;
//    }
//
//    fatal_assert(queue_size < METADATA_DATABASE_CMD_Q_MAX_SIZE);
//    /* enqueue command */
//    wc->cmd_queue.cmd_array[wc->cmd_queue.tail] = *cmd;
//    wc->cmd_queue.tail = wc->cmd_queue.tail != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
//                             wc->cmd_queue.tail + 1 : 0;
//    wc->queue_size = queue_size + 1;
//    uv_mutex_unlock(&wc->cmd_mutex);
//    return 0;
//}

void metadata_database_enq_cmd(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
{
    unsigned queue_size;

    /* wait for free space in queue */
    uv_mutex_lock(&wc->cmd_mutex);
    if (wc->is_shutting_down) {
        uv_mutex_unlock(&wc->cmd_mutex);
        return;
    }

    while ((queue_size = wc->queue_size) == METADATA_DATABASE_CMD_Q_MAX_SIZE) {
        uv_cond_wait(&wc->cmd_cond, &wc->cmd_mutex);
    }
    fatal_assert(queue_size < METADATA_DATABASE_CMD_Q_MAX_SIZE);
    /* enqueue command */
    wc->cmd_queue.cmd_array[wc->cmd_queue.tail] = *cmd;
    wc->cmd_queue.tail = wc->cmd_queue.tail != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
                             wc->cmd_queue.tail + 1 : 0;
    wc->queue_size = queue_size + 1;
    uv_mutex_unlock(&wc->cmd_mutex);

    /* wake up event loop */
    (void) uv_async_send(&wc->async);
}

struct metadata_database_cmd metadata_database_deq_cmd(struct metadata_database_worker_config* wc)
{
    struct metadata_database_cmd ret;
    unsigned queue_size;

    uv_mutex_lock(&wc->cmd_mutex);
    queue_size = wc->queue_size;
    if (queue_size == 0 || wc->is_shutting_down) {
        memset(&ret, 0, sizeof(ret));
        ret.opcode = METADATA_DATABASE_NOOP;
        ret.completion = NULL;
        if (wc->is_shutting_down)
            uv_cond_signal(&wc->cmd_cond);
    } else {
        /* dequeue command */
        ret = wc->cmd_queue.cmd_array[wc->cmd_queue.head];
        if (queue_size == 1) {
            wc->cmd_queue.head = wc->cmd_queue.tail = 0;
        } else {
            wc->cmd_queue.head = wc->cmd_queue.head != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
                                     wc->cmd_queue.head + 1 : 0;
        }
        wc->queue_size = queue_size - 1;
        /* wake up producers */
        uv_cond_signal(&wc->cmd_cond);
    }
    uv_mutex_unlock(&wc->cmd_mutex);

    return ret;
}

static void async_cb(uv_async_t *handle)
{
    uv_stop(handle->loop);
    uv_update_time(handle->loop);
}

#define TIMER_INITIAL_PERIOD_MS (1000)
#define TIMER_REPEAT_PERIOD_MS (1000)

// 120000 /10000
//static void timer_cb1(uv_timer_t* handle)
//{
//    uv_stop(handle->loop);
//    uv_update_time(handle->loop);
//
//    struct metadata_database_worker_config *wc = handle->data;
//    wc->wakeup_now = 1;
////    wc->max_batch += 32;
////    if (wc->max_batch > 256)
////        wc->max_batch = 256;
//    //    struct metadata_database_cmd cmd;
//    //    memset(&cmd, 0, sizeof(cmd));
//    //    cmd.opcode = METADATA_DATABASE_TIMER;
//    //    metadata_database_enq_cmd_noblock(wc, &cmd);
//}

static void timer_cb(uv_timer_t* handle)
{
    uv_stop(handle->loop);
    uv_update_time(handle->loop);

   struct metadata_database_worker_config *wc = handle->data;
//    wc->max_batch += 32;
//    struct metadata_database_cmd cmd;
//    memset(&cmd, 0, sizeof(cmd));
//    cmd.opcode = METADATA_DATABASE_TIMER;
//    metadata_database_enq_cmd_noblock(wc, &cmd);
    wc->wakeup_now = 1;
}

#define MAX_CMD_BATCH_SIZE (32)

void metadata_database_worker(void *arg)
{
    worker_register("METASYNC");
    worker_register_job_name(METADATA_DATABASE_NOOP,        "noop");
    worker_register_job_name(METADATA_DATABASE_TIMER,       "timer");
    worker_register_job_name(METADATA_ADD_CHART,            "add chart");
    worker_register_job_name(METADATA_ADD_CHART_LABEL,      "add chart label");
    worker_register_job_name(METADATA_ADD_CHART_ACTIVE,     "add chart active");
    worker_register_job_name(METADATA_ADD_CHART_HASH,       "add chart hash");
    worker_register_job_name(METADATA_ADD_DIMENSION,        "add dimension");
    worker_register_job_name(METADATA_DEL_DIMENSION,        "delete dimension");
    worker_register_job_name(METADATA_ADD_DIMENSION_ACTIVE, "add dimension active");
    worker_register_job_name(METADATA_ADD_DIMENSION_OPTION, "dimension option");

    struct metadata_database_worker_config *wc = arg;
    uv_loop_t *loop;
    int ret;
    enum metadata_database_opcode opcode;
    uv_timer_t timer_req;
//    uv_timer_t timer_req1;
    struct metadata_database_cmd cmd;
    unsigned cmd_batch_size;

    uv_thread_set_name_np(wc->thread, "METASYNC");
    loop = wc->loop = mallocz(sizeof(uv_loop_t));
    ret = uv_loop_init(loop);
    if (ret) {
        error("uv_loop_init(): %s", uv_strerror(ret));
        goto error_after_loop_init;
    }
    loop->data = wc;

    ret = uv_async_init(wc->loop, &wc->async, async_cb);
    if (ret) {
        error("uv_async_init(): %s", uv_strerror(ret));
        goto error_after_async_init;
    }
    wc->async.data = wc;

    ret = uv_timer_init(loop, &timer_req);
    if (ret) {
        error("uv_timer_init(): %s", uv_strerror(ret));
        goto error_after_timer_init;
    }
    timer_req.data = wc;
    fatal_assert(0 == uv_timer_start(&timer_req, timer_cb, TIMER_INITIAL_PERIOD_MS, TIMER_REPEAT_PERIOD_MS));

//    ret = uv_timer_init(loop, &timer_req1);
//    if (ret) {
//        error("uv_timer_init(): %s", uv_strerror(ret));
//        goto error_after_timer_init;
//    }
//    timer_req1.data = wc;
//    fatal_assert(0 == uv_timer_start(&timer_req1, timer_cb1, 120000, 11000));

    info("Starting metadata sync thread -- scratch area %d entries, %lu bytes", METADATA_DATABASE_CMD_Q_MAX_SIZE, sizeof(*wc));

    memset(&cmd, 0, sizeof(cmd));
    wc->startup_time = now_realtime_sec();
    wc->max_batch = 256;

    unsigned int max_commands_in_queue = 0;
    while (likely(!netdata_exit)) {
        RRDDIM *rd;
        RRDSET *st;
        uuid_t  *uuid;
        int rc;

        worker_is_idle();
        uv_run(loop, UV_RUN_DEFAULT);

        /* wait for commands */
        cmd_batch_size = 0;
        //db_execute("BEGIN TRANSACTION;");
        do {
            if (unlikely(cmd_batch_size >= wc->max_batch))
                break;
            cmd = metadata_database_deq_cmd(wc);

            if (netdata_exit)
                break;

            opcode = cmd.opcode;
            ++cmd_batch_size;

            if (wc->queue_size > max_commands_in_queue) {
                max_commands_in_queue = wc->queue_size;
                info("Maximum commands in metadata queue = %u", max_commands_in_queue);
            }

            if (!wc->wakeup_now) {
                worker_is_idle();
                usleep(50 * USEC_PER_MS);
            }

            if (likely(opcode != METADATA_DATABASE_NOOP)) {
                if (opcode == METADATA_ADD_CHART_FULL)
                    worker_is_busy(METADATA_ADD_CHART);
                else
                    worker_is_busy(opcode);
            }

            switch (opcode) {
                case METADATA_DATABASE_NOOP:
                    /* the command queue was empty, do nothing */
                    break;
                case METADATA_DATABASE_TIMER:
                    /* the command queue was empty, do nothing */
                    //info("Metadata timer tick!");
                    break;
                case METADATA_ADD_CHART:
                    st = (RRDSET *) cmd.param[0];
                    update_chart_metadata(st->chart_uuid, st, (char *) cmd.param[1], (char *) cmd.param[2]);
                    freez(cmd.param[1]);
                    freez(cmd.param[2]);
                    rrd_atomic_fetch_add(&st->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_CHART_FULL:
                    st = (RRDSET *) cmd.param[0];
                    update_chart_metadata(st->chart_uuid, st, (char *) cmd.param[1], (char *) cmd.param[2]);

                    worker_is_busy(METADATA_ADD_CHART_ACTIVE);
                    store_active_chart(st->chart_uuid);

                    worker_is_busy(METADATA_ADD_CHART_HASH);
                    compute_chart_hash(st, 1);

                    freez(cmd.param[1]);
                    freez(cmd.param[2]);
                    rrd_atomic_fetch_add(&st->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_CHART_LABEL:
                    st = (RRDSET *) cmd.param[0];
                    struct label_index *labels = &st->state->labels;
                    netdata_rwlock_rdlock(&labels->labels_rwlock);
                    struct label *lbl = labels->head;
                    while (lbl) {
                        sql_store_chart_label(st->chart_uuid, (int)lbl->label_source, lbl->key, lbl->value);
                        lbl = lbl->next;
                    }
                    netdata_rwlock_unlock(&labels->labels_rwlock);
                    rrd_atomic_fetch_add(&st->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_CHART_ACTIVE:
                    st = (RRDSET *) cmd.param[0];
                    store_active_chart(st->chart_uuid);
                    rrd_atomic_fetch_add(&st->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_CHART_HASH:
                    st = (RRDSET *) cmd.param[0];
                    compute_chart_hash(st, 1);
                    rrd_atomic_fetch_add(&st->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_DIMENSION:
                    rd = (RRDDIM *) cmd.param[0];
                    //uuid = (uuid_t *) cmd.param[1];
                    rc = sql_store_dimension(&rd->state->metric_uuid, rd->rrdset->chart_uuid, rd->id, rd->name, rd->multiplier, rd->divisor, rd->algorithm);
                    if (unlikely(rc))
                        error_report("Failed to store dimension %s", rd->id);
                    //freez(uuid);
                    rrd_atomic_fetch_add(&rd->state->metadata_update_count, -1);
                    break;
                case METADATA_DEL_DIMENSION:
                    uuid = (uuid_t *) cmd.param[0];
                    delete_dimension_uuid(uuid);
                    freez(uuid);
                    break;
                case METADATA_ADD_DIMENSION_ACTIVE:
                    rd = (RRDDIM *) cmd.param[0];
                    store_active_dimension(&rd->state->metric_uuid);
                    rrd_atomic_fetch_add(&rd->state->metadata_update_count, -1);
                    break;
                case METADATA_ADD_DIMENSION_OPTION:
                    rd = (RRDDIM *) cmd.param[0];
                    //info("Adding dimension %s option", rd->id);
                    if (likely(!cmd.param[1]))
                        (void)sql_set_dimension_option(&rd->state->metric_uuid, NULL);
                    else
                        (void)sql_set_dimension_option(&rd->state->metric_uuid, (char *) cmd.param[1]);
                    freez(cmd.param[1]);
                    rrd_atomic_fetch_add(&rd->state->metadata_update_count, -1);
                    break;
                default:
                    break;
            }
            if (cmd.completion)
                metadata_complete(cmd.completion);
        } while (opcode != METADATA_DATABASE_NOOP);
        //db_execute("COMMIT TRANSACTION;");
    }

    if (!uv_timer_stop(&timer_req))
        uv_close((uv_handle_t *)&timer_req, NULL);

//    if (!uv_timer_stop(&timer_req1))
//        uv_close((uv_handle_t *)&timer_req1, NULL);

    /*
     * uv_async_send after uv_close does not seem to crash in linux at the moment,
     * it is however undocumented behaviour we need to be aware if this becomes
     * an issue in the future.
     */
    uv_close((uv_handle_t *)&wc->async, NULL);
    uv_run(loop, UV_RUN_DEFAULT);

    info("Shutting down metadata event loop. Maximum commands in queue %u", max_commands_in_queue);
    /* TODO: don't let the API block by waiting to enqueue commands */
    uv_cond_destroy(&wc->cmd_cond);
    /*  uv_mutex_destroy(&wc->cmd_mutex); */
    //fatal_assert(0 == uv_loop_close(loop));
    int rc;

    do {
        rc = uv_loop_close(loop);
    } while (rc != UV_EBUSY);

    freez(loop);
    worker_unregister();
    return;

error_after_timer_init:
    uv_close((uv_handle_t *)&wc->async, NULL);
error_after_async_init:
    fatal_assert(0 == uv_loop_close(loop));
error_after_loop_init:
    freez(loop);
    worker_unregister();
}

// -------------------------------------------------------------

void metadata_sync_init(struct metadata_database_worker_config *wc)
{

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE) {
            return;
        }
        error_report("Database has not been initialized");
        return;
    }

    fatal_assert(0 == uv_mutex_init(&metadata_async_lock));

    memset(wc, 0, sizeof(*wc));
    metadata_database_init_cmd_queue(wc);
    fatal_assert(0 == uv_thread_create(&(wc->thread), metadata_database_worker, wc));
    info("SQLite metadata sync initialization completed");
    return;
}
