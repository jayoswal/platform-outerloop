from typing import Any

import jwt
from fastapi import Depends, Header, HTTPException, status

from .config import settings

Claims = dict[str, Any]


def current_user(authorization: str = Header(default="")) -> Claims:
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            {"error": {"code": "UNAUTHENTICATED", "message": "Missing bearer token"}},
        )
    try:
        claims: Claims = jwt.decode(
            authorization[7:], settings.jwt_secret, algorithms=["HS256"]
        )
        return claims
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            {"error": {"code": "UNAUTHENTICATED", "message": "Invalid token"}},
        ) from exc


def require(*roles: str) -> Any:
    def dependency(user: Claims = Depends(current_user)) -> Claims:
        if roles and not set(roles).intersection(user.get("roles", [])):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                {"error": {"code": "FORBIDDEN", "message": "Insufficient role"}},
            )
        return user

    return dependency

