<?xml version="1.0"?>
<!--
  Storage policy "tiered": volume hot = local PVC (disk default), volume cold = S3 bucket on
  SeaweedFS behind a small local read cache. Tables move parts with TTL ... TO VOLUME 'cold'
  (schema/20-ttl.sql.tpl). Credentials come from the pod environment (Secret seaweedfs-s3).
-->
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_cold>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>      <!-- part metadata on the PVC, objects in S3 -->
                <endpoint>${CLICKHOUSE_S3_ENDPOINT}</endpoint>
                <access_key_id from_env="S3_ACCESS_KEY_ID"/>
                <secret_access_key from_env="S3_SECRET_ACCESS_KEY"/>
                <region>us-east-1</region>
                <!-- Start even while SeaweedFS is down: only cold-part reads/moves need it. -->
                <skip_access_check>true</skip_access_check>
                <metadata_path>/var/lib/clickhouse/disks/s3_cold/</metadata_path>
            </s3_cold>
            <s3_cold_cache>
                <type>cache</type>
                <disk>s3_cold</disk>
                <path>/var/lib/clickhouse/disks/s3_cold_cache/</path>
                <max_size>${CLICKHOUSE_S3_CACHE_SIZE}</max_size>
                <cache_on_write_operations>0</cache_on_write_operations>  <!-- moves to S3 do not fill the cache -->
            </s3_cold_cache>
        </disks>
        <policies>
            <tiered>
                <volumes>
                    <hot>
                        <disk>default</disk>
                    </hot>
                    <cold>
                        <disk>s3_cold_cache</disk>
                        <!-- Parts arrive merged from hot; no rewrite of objects on S3. -->
                        <prefer_not_to_merge>true</prefer_not_to_merge>
                        <!-- Late/backlog rows land on hot first (merged there), then move. -->
                        <perform_ttl_move_on_insert>false</perform_ttl_move_on_insert>
                    </cold>
                </volumes>
                <!-- Safety valve: move the oldest parts to cold when the PVC has < 10% free. -->
                <move_factor>0.1</move_factor>
            </tiered>
        </policies>
    </storage_configuration>
</clickhouse>
