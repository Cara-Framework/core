from __future__ import annotations

class BaseScope:
    def on_boot(self, builder):
        raise NotImplementedError()

    def on_remove(self, builder):
        raise NotImplementedError()
