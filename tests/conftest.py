"""测试公共配置：保证单测运行时打印入队标识、租约信息与状态流转判定依据。"""

import logging
import sys

_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%H:%M:%S")
)
_svc_logger = logging.getLogger("task_store.service")
_svc_logger.setLevel(logging.INFO)
_svc_logger.addHandler(_handler)
_svc_logger.propagate = False
