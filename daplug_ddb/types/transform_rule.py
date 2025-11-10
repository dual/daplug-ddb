from typing import Any, Callable, Optional, Tuple

TransformRule = Tuple[str, type[Any], Callable[[Any, bool], Optional[Any]]]
