
from .sujet_router import router as sujet_router
from .action_router import router as action_router
from .directory_router import router as directory_router
from .dashboard_router import router as dashboard_router
from .auth_router import router as auth_router

ALL_ROUTERS = [
    sujet_router,
    directory_router,
    dashboard_router,
    auth_router,
    action_router
]