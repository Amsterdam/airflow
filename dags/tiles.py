import airflow
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret

### Configmap Trex

configmap ="trex-env"
env_from = [k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=configmap
                )
            )]

### Volume mounts

volume_mount_data_trex = k8s.V1VolumeMount(
    name='trex-volume-data', mount_path='/var/cache/mvtcache', sub_path=None, read_only=False)

volume_mount_data_mapproxy = k8s.V1VolumeMount(
    name='mapproxy-volume-data', mount_path='/mnt/tiles', sub_path=None, read_only=False)

volume_mount_config = k8s.V1VolumeMount(
    name='volume-cm', mount_path='/var/config', sub_path=None, read_only=True)

### Volume claims

volume_data_trex = k8s.V1Volume(
    name='trex-volume-data',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='trex-volume-data'),
)

volume_data_mapproxy = k8s.V1Volume(
    name='test-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='mapproxy-volume-data'),
)

volume_config_trex = k8s.V1Volume(
    name='volume-cm',
    config_map=k8s.V1ConfigMapVolumeSource(name='trex-config'),
)

volume_config_mapproxy = k8s.V1Volume(
    name='volume-cm',
    config_map=k8s.V1ConfigMapVolumeSource(name='mapproxy-config'),
)

### Pods resource config

fullresources=k8s.V1ResourceRequirements(
    requests={
        'memory': '6Gi',
        'cpu': '2500m',
    },
    limits={
        'memory': '12Gi',
        'cpu': '3000m',
    }
)

### DAG Start
with DAG(
    dag_id='tiles',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['tiles'],
) as dag:
    trex_generate_pbf_wm = KubernetesPodOperator(
        name="trex_generate_pbf_wm",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="sourcepole/t-rex",
        namespace="tiles",
        arguments=["generate", "--loglevel", "debug", "--minzoom", "10", "--maxzoom", "16", "--extent", "4.49712476945351,52.1630507756721,5.60867873764429,52.6147675426215", "--config", "/var/config/topo_wm.toml"],
        task_id="trex_generate_pbf_wm",
        volumes=[volume_config_trex, volume_data_trex],
        volume_mounts=[volume_mount_config, volume_mount_data_trex],
        security_context=dict(fsGroup=33),
        env_from=env_from,
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=False,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    trex_generate_pbf_rd = KubernetesPodOperator(
        name="trex_generate_pbf_rd",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="sourcepole/t-rex",
        namespace="tiles",
        arguments=["generate", "--loglevel", "debug", "--minzoom", "5", "--maxzoom", "11", "--extent", "4.49712476945351,52.1630507756721,5.60867873764429,52.6147675426215", "--config", "/var/config/topo_rd.toml"],
        task_id="trex_generate_pbf_rd",
        volumes=[volume_config_trex, volume_data_trex],
        volume_mounts=[volume_mount_config, volume_mount_data_trex],
        security_context=dict(fsGroup=33),
        env_from=env_from,
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=False,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    upload_pbf_wm = KubernetesPodOperator(
        name="upload_pbf_wm",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/var/cache/mvtcache/wm/*' https://piosupportstor.blob.core.windows.net/tiles/wm/pbf/ --recursive --content-encoding gzip --content-type application/vnd.mapbox-vector-tile"],
        task_id="upload_pbf_wm",
        volumes=[volume_data_trex],
        volume_mounts=[volume_mount_data_trex],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    upload_pbf_rd = KubernetesPodOperator(
        name="upload_pbf_rd",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/var/cache/mvtcache/rd/*' https://piosupportstor.blob.core.windows.net/tiles/rd/pbf/ --recursive --content-encoding gzip --content-type application/vnd.mapbox-vector-tile"],
        task_id="upload_pbf_rd",
        volumes=[volume_data_trex],
        volume_mounts=[volume_mount_data_trex],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )    
    mapproxy_generate_tiles_wm = KubernetesPodOperator(
        name="mapproxy_generate_tiles_wm",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "4", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=wm_kbk"],
        task_id="mapproxy_generate_tiles_wm",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles_wm_zw = KubernetesPodOperator(
        name="mapproxy_generate_tiles_wm_zw",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "4", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=wm_kbk_zw"],
        task_id="mapproxy_generate_tiles_wm_zw",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles_wm_light = KubernetesPodOperator(
        name="mapproxy_generate_tiles_wm_light",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "4", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=wm_kbk_light"],
        task_id="mapproxy_generate_tiles_wm_light",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles_rd = KubernetesPodOperator(
        name="mapproxy_generate_tiles_rd",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "2", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk"],
        task_id="mapproxy_generate_tiles_rd",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles_rd_zw = KubernetesPodOperator(
        name="mapproxy_generate_tiles_rd_zw",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "2", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk_zw"],
        task_id="mapproxy_generate_tiles_rd_zw",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles_rd_light = KubernetesPodOperator(
        name="mapproxy_generate_tiles_rd_light",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "2", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk_light"],
        task_id="mapproxy_generate_tiles_rd_light",
        volumes=[volume_config_mapproxy, volume_data_mapproxy],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        resources=fullresources,
        startup_timeout_seconds=300,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_wm = KubernetesPodOperator(
        name="upload_tiles_wm",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_wm_seed_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/wm/default/ --recursive"],
        task_id="upload_tiles_wm",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_wm_zw = KubernetesPodOperator(
        name="upload_tiles_wm_zw",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_wm_seed_zw_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/wm/zw/ --recursive"],
        task_id="upload_tiles_wm_zw",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_wm_light = KubernetesPodOperator(
        name="upload_tiles_wm_light",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_wm_seed_light_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/wm/light/ --recursive"],
        task_id="upload_tiles_wm_light",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_rd = KubernetesPodOperator(
        name="upload_tiles_rd",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/rd/default/ --recursive"],
        task_id="upload_tiles_rd",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_rd_zw = KubernetesPodOperator(
        name="upload_tiles_rd_zw",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_zw_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/rd/zw/ --recursive"],
        task_id="upload_tiles_rd_zw",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles_rd_light = KubernetesPodOperator(
        name="upload_tiles_rd_light",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_light_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/rd/light/ --recursive"],
        task_id="upload_tiles_rd_light",
        volumes=[volume_data_mapproxy],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        node_selectors={"nodetype": "tiles"},
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False
    )

trex_generate_pbf_wm >> upload_pbf_wm
upload_pbf_wm >> mapproxy_generate_tiles_wm >> upload_tiles_wm
upload_pbf_wm >> mapproxy_generate_tiles_wm_zw >> upload_tiles_wm_zw
upload_pbf_wm >> mapproxy_generate_tiles_wm_light >> upload_tiles_wm_light 
trex_generate_pbf_rd >> upload_pbf_rd
upload_pbf_rd >> mapproxy_generate_tiles_rd >> upload_tiles_rd 
upload_pbf_rd >> mapproxy_generate_tiles_rd_zw >> upload_tiles_rd_zw
upload_pbf_rd >> mapproxy_generate_tiles_rd_light >> upload_tiles_rd_light
