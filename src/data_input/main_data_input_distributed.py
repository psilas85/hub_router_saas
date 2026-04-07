#hub_router_1.0.1/src/data_input/main_data_input_distributed.py

import argparse
import time
from rq.job import Job
from redis import Redis

from data_input.application.data_input_distributed_use_case import DataInputDistributedUseCase


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--arquivo", required=True)

    args = parser.parse_args()

    tenant_id = args.tenant
    filepath = args.arquivo

    use_case = DataInputDistributedUseCase(tenant_id)

    orchestrator = use_case.execute(filepath)
    subjobs = orchestrator["subjobs"]

    redis_conn = Redis(host="redis", port=6379)

    print(f"🚀 {len(subjobs)} subjobs criados")

    total_valid = 0
    total_invalid = 0
    total_processed = 0

    while True:

        finished = 0

        for jid in subjobs:
            job = Job.fetch(jid, connection=redis_conn)

            if job.is_finished:
                finished += 1

                if job.result:
                    total_processed += len(job.result.get("results", []))

        if finished == len(subjobs):
            break

        print(f"⏳ {finished}/{len(subjobs)} finalizados")
        time.sleep(2)

    print({
        "validos": total_valid,
        "invalidos": total_invalid
    })


if __name__ == "__main__":
    main()