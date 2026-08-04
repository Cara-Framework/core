from cara.commands.core.RouteGeneratorCommand import RouteGeneratorCommand


def test_route_group_keeps_security_chain_on_one_line() -> None:
    command = object.__new__(RouteGeneratorCommand)

    generated = command._generate_route_group(
        {
            "prefix": "/admin",
            "middleware": ["auth.session", "admin", "two_factor"],
            "routes": [
                {
                    "methods": [
                        {
                            "http_method": "GET",
                            "controller_method": "show",
                            "path": "/session",
                            "as": "admin.session.show",
                        }
                    ]
                }
            ],
        },
        "AdminSessionController",
    )

    assert generated.startswith(
        'Route.prefix("/admin").middleware(["auth.session", "admin", '
        '"two_factor"]).routes('
    )
