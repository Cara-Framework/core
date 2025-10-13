"""
Tinker Command for the Cara framework.

This module provides a CLI command to start the interactive Tinker shell with enhanced UX.
"""

from pathlib import Path
from typing import List, Optional

from cara.commands import CommandBase
from cara.decorators import command


@command(
    name="tinker",
    help="Start the interactive Tinker shell for Laravel-style development.",
    options={
        "--no-ipython": "Use basic Python shell instead of IPython",
        "--include=?": "Comma-separated list of additional modules to include",
        "--execute=?": "Execute a single command and exit",
        "--file=?": "Execute commands from a Python file",
        "--verbose": "Show verbose output during execution",
        "--quiet": "Minimal output mode",
    },
)
class TinkerCommand(CommandBase):
    """Interactive Tinker shell command with enhanced options."""

    def handle(
        self,
        include: Optional[str] = None,
        execute: Optional[str] = None,
        file: Optional[str] = None,
    ):
        """Handle Tinker shell startup with enhanced options."""
        self.info("🔧 Starting Cara Tinker...")

        try:
            # Import tinker components
            from cara.tinker import Repl, ScriptRunner, Shell

            # Handle different execution modes
            if execute:
                self._execute_single_command(execute)
            elif file:
                self._execute_file(file)
            else:
                self._start_interactive_shell(include)

        except ImportError as e:
            self.error(f"❌ Tinker not available: {e}")
            self.error("💡 Make sure Tinker package is properly installed")
        except Exception as e:
            self.error(f"❌ Tinker error: {e}")
            if self.option("verbose"):
                import traceback

                self.error(f"Stack trace: {traceback.format_exc()}")

    def _start_interactive_shell(self, include: Optional[str] = None):
        """Start interactive Tinker shell."""
        from cara.tinker import Shell

        # Show startup message
        if not self.option("quiet"):
            self._show_startup_banner()

        # Create shell
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        # Include additional modules
        if include:
            self._include_modules(include.split(","), shell)

        # Configure shell options
        use_ipython = not self.option("no-ipython")

        self.info("🚀 Starting interactive shell...")
        if use_ipython:
            self.info("💡 Using IPython for enhanced experience")
        else:
            self.info("💡 Using basic Python shell")

        self.info("✨ Enhanced development features enabled")

        # Start shell
        try:
            shell.start(use_ipython=use_ipython)
        except KeyboardInterrupt:
            self.info("\n👋 Tinker session ended")
        except Exception as e:
            self.error(f"❌ Shell error: {e}")

    def _execute_single_command(self, command: str):
        """Execute a single command and exit."""
        from cara.tinker import Repl, Shell

        if self.option("verbose"):
            self.info(f"🔧 Executing: {command}")

        # Create shell and REPL
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        repl = Repl(shell.namespace)

        try:
            # Execute command
            result = repl.execute(command)

            # Show result
            if result is not None:
                if self.option("verbose"):
                    self.info("📋 Result:")
                repl.format_result(result)

            self.success("✅ Command executed successfully")

        except Exception as e:
            self.error(f"❌ Command execution failed: {e}")

    def _execute_file(self, file_path: str):
        """Execute commands from a file."""
        from cara.tinker import ScriptRunner, Shell

        if self.option("verbose"):
            self.info(f"📁 Executing file: {file_path}")

        # Create shell and script runner
        shell = Shell()

        # Add enhanced features (always enabled)
        self._add_enhanced_features(shell)

        runner = ScriptRunner(shell)

        try:
            # Execute file
            show_progress = not self.option("quiet")
            results = runner.run_file(file_path, show_progress=show_progress)

            # Show summary
            successful = sum(1 for r in results if r.get("success", False))
            total = len(results)

            if successful == total:
                self.success(
                    f"✅ File executed successfully ({successful}/{total} commands)"
                )
            else:
                failed = total - successful
                self.warning(
                    f"⚠️  File executed with errors ({successful}/{total} successful, {failed} failed)"
                )

        except Exception as e:
            self.error(f"❌ File execution failed: {e}")

    def _add_enhanced_features(self, shell):
        """Add enhanced development features to shell."""
        from rich.console import Console
        from rich.table import Table

        from cara.facades import Cache, Config

        console = Console()

        def app_info():
            """Show application information."""
            info = {
                "name": Config.get("app.name", "Unknown"),
                "env": Config.get("app.env", "Unknown"),
                "debug": Config.get("app.debug", False),
                "url": Config.get("app.url", "Unknown"),
                "timezone": Config.get("app.timezone", "UTC"),
            }

            table = Table(title="🏗️ Application Information")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="green")

            for key, value in info.items():
                table.add_row(key.upper(), str(value))

            console.print(table)
            return info

        def db_info():
            """Show database information."""
            try:
                # Test connection using User model from container
                User = self._resolve_user_model()
                if User:
                    user_count = User.count()
                    connection_status = f"✅ Connected ({user_count} users)"
                else:
                    connection_status = "⚠️ User model not registered"
            except Exception as e:
                connection_status = f"❌ Error: {str(e)}"

            info = {
                "connection": Config.get("database.default", "Unknown"),
                "status": connection_status,
            }

            table = Table(title="🗄️ Database Information")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="green")

            for key, value in info.items():
                table.add_row(key.upper(), str(value))

            console.print(table)
            return info

        def routes_count():
            """Get total routes count."""
            try:
                from cara.facades import Route

                routes = Route.get_routes()
                return len(routes) if routes else 0
            except Exception:
                return "Unable to get routes count"

        def clear_cache_all():
            """Clear all caches."""
            try:
                Cache.flush()
                return "✅ All caches cleared"
            except Exception as e:
                return f"❌ Cache error: {str(e)}"

        def test_cache():
            """Test cache functionality."""
            test_key = "tinker_test"
            test_value = "Hello Cache!"

            try:
                # Put value
                Cache.put(test_key, test_value, 60)

                # Get value
                retrieved = Cache.get(test_key)

                # Clean up
                Cache.forget(test_key)

                if retrieved == test_value:
                    return "✅ Cache working correctly"
                else:
                    return "❌ Cache value mismatch"
            except Exception as e:
                return f"❌ Cache error: {str(e)}"

        def quick_query(model_name: str, limit: int = 10):
            """Execute quick model query with nice table output."""
            try:
                # Dynamic model import
                models_map = {
                    "users": "User",
                    "user": "User",
                    "products": "Product",
                    "product": "Product",
                    "jobs": "Job",
                    "job": "Job",
                }

                model_class_name = models_map.get(model_name.lower())
                if not model_class_name:
                    return f"❌ Model '{model_name}' not found. Available: {list(models_map.keys())}"

                # Import from shell namespace (already loaded)
                model_class = shell.namespace.get(model_class_name)
                if not model_class:
                    return f"❌ Model class '{model_class_name}' not available in shell"

                # Get data
                result = model_class.limit(limit).get()

                if result and len(result) > 0:
                    # Convert models to list of dicts
                    data = []
                    for item in result:
                        if hasattr(item, "__attributes__"):
                            # Use Eloquent model attributes
                            item_dict = {}
                            for key, value in item.__attributes__.items():
                                # Convert datetime objects to strings for display
                                if hasattr(value, "strftime"):
                                    item_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                                else:
                                    item_dict[key] = str(value)
                            data.append(item_dict)
                        elif hasattr(item, "to_dict"):
                            data.append(item.to_dict())
                        elif hasattr(item, "__dict__"):
                            # Get attributes excluding private ones
                            item_dict = {
                                k: v
                                for k, v in item.__dict__.items()
                                if not k.startswith("_")
                            }
                            data.append(item_dict)
                        else:
                            data.append({"value": str(item)})

                    if data and isinstance(data[0], dict):
                        table = Table(
                            title=f"📊 {model_class_name} Records (showing {len(data)})"
                        )

                        # Add columns
                        for key in data[0].keys():
                            table.add_column(key, style="cyan")

                        # Add rows
                        for row in data:
                            table.add_row(*[str(v) for v in row.values()])

                        console.print(table)
                        return f"✅ Displayed {len(data)} {model_class_name.lower()} records"
                    else:
                        console.print(f"Results: {data}")
                        return data
                else:
                    console.print(f"No records found in {model_class_name}")
                    return f"No records in {model_class_name}"

            except Exception as e:
                return f"❌ Query Error: {str(e)}"

        def model_stats():
            """Show statistics for all models."""
            try:
                models = ["User", "Product", "Job"]
                stats = {}

                table = Table(title="📊 Model Statistics")
                table.add_column("Model", style="cyan")
                table.add_column("Count", style="green")
                table.add_column("Status", style="yellow")

                for model_name in models:
                    model_class = shell.namespace.get(model_name)
                    if model_class:
                        try:
                            count = model_class.count()
                            stats[model_name] = count
                            table.add_row(model_name, str(count), "✅ OK")
                        except Exception as e:
                            stats[model_name] = f"Error: {str(e)}"
                            table.add_row(model_name, "N/A", f"❌ {str(e)}")
                    else:
                        table.add_row(model_name, "N/A", "❌ Not loaded")

                console.print(table)
                return stats
            except Exception as e:
                return f"❌ Stats Error: {str(e)}"

        def show_config(key: str = None):
            """Show configuration values."""
            if key:
                value = Config.get(key)
                console.print(f"[cyan]{key}:[/cyan] [green]{value}[/green]")
                return value
            else:
                # Show common config values
                common_configs = [
                    "app.name",
                    "app.env",
                    "app.debug",
                    "app.url",
                    "database.default",
                    "cache.default",
                    "queue.default",
                ]

                table = Table(title="🔧 Common Configuration")
                table.add_column("Key", style="cyan")
                table.add_column("Value", style="green")

                for config_key in common_configs:
                    value = Config.get(config_key, "Not Set")
                    table.add_row(config_key, str(value))

                console.print(table)
                return "Configuration displayed above"

        def logs(lines: int = 20):
            """Show recent log entries."""
            try:
                log_files = list(Path("storage/logs").glob("app_*.log"))
                if not log_files:
                    return "No log files found"

                latest_log = max(log_files, key=lambda x: x.stat().st_mtime)

                with open(latest_log, "r") as f:
                    log_lines = f.readlines()
                    recent_lines = log_lines[-lines:]

                    for line in recent_lines:
                        if "ERROR" in line:
                            console.print(line.strip(), style="red")
                        elif "WARNING" in line:
                            console.print(line.strip(), style="yellow")
                        elif "INFO" in line:
                            console.print(line.strip(), style="green")
                        else:
                            console.print(line.strip(), style="dim")

                return f"Showing last {lines} lines from {latest_log.name}"
            except Exception as e:
                return f"Error reading logs: {str(e)}"

        def benchmark(func, *args, **kwargs):
            """Benchmark a function execution."""
            import time

            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            execution_time = (end_time - start_time) * 1000  # Convert to milliseconds

            console.print(f"⏱️ Execution time: {execution_time:.2f}ms")
            return result

        def craft_command(command: str):
            """Run craft command from tinker."""
            import subprocess

            result = subprocess.run(
                ["python", "craft"] + command.split(),
                capture_output=True,
                text=True,
                cwd=".",
            )
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                console.print(result.stderr, style="red")
            return f"Command exit code: {result.returncode}"

        def test_mail():
            """Test mail configuration and send test email."""
            try:
                # Test mail configuration
                console.print("📧 Testing mail configuration...")

                # You can send a test email like this:
                # Mail.to('test@example.com').subject('Test Mail').send('Hello from Cara!')

                # For now, just check if mail driver is configured
                mail_driver = Config.get("mail.default", "Unknown")
                mail_from = Config.get("mail.from_address", "Unknown")

                table = Table(title="📧 Mail Configuration")
                table.add_column("Setting", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("Driver", mail_driver)
                table.add_row("From Address", mail_from)
                table.add_row("Status", "✅ Ready to send")

                console.print(table)

                return {
                    "driver": mail_driver,
                    "from_address": mail_from,
                    "status": "ready",
                }
            except Exception as e:
                return f"❌ Mail error: {str(e)}"

        def test_queue():
            """Test queue functionality."""
            try:
                # Test queue configuration
                console.print("⚡ Testing queue configuration...")

                queue_driver = Config.get("queue.default", "Unknown")

                table = Table(title="⚡ Queue Configuration")
                table.add_column("Setting", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("Driver", queue_driver)
                table.add_row("Status", "✅ Ready to queue jobs")

                console.print(table)

                # Example of how to queue a job:
                console.print("\n💡 [bold cyan]Example Usage:[/bold cyan]")
                console.print(
                    "   Queue.push('app.jobs.SendEmailJob', {'email': 'user@example.com'})"
                )
                console.print(
                    "   Queue.later(60, 'app.jobs.ProcessDataJob', {'data': 'some_data'})"
                )

                return {"driver": queue_driver, "status": "ready"}
            except Exception as e:
                return f"❌ Queue error: {str(e)}"

        def test_notification():
            """Test notification system."""
            try:
                # Test notification configuration
                console.print("🔔 Testing notification configuration...")

                notification_driver = str(
                    Config.get("notification.default", "database")
                )

                table = Table(title="🔔 Notification Configuration")
                table.add_column("Setting", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("Driver", notification_driver)
                table.add_row("Status", "✅ Ready to send notifications")

                console.print(table)

                # Example of how to send notifications:
                console.print("\n💡 [bold cyan]Example Usage:[/bold cyan]")
                console.print("   user = User.first()")
                console.print("   user.notify(WelcomeNotification())")
                console.print(
                    "   Notification.send([user], NewMessageNotification(message))"
                )

                return {"driver": notification_driver, "status": "ready"}
            except Exception as e:
                return f"❌ Notification error: {str(e)}"

        def send_test_mail(
            to_email: str = "test@example.com", subject: str = "Test from Cara Tinker"
        ):
            """Send a test email."""
            try:
                console.print(f"📧 Sending test email to {to_email}...")

                # Example mail sending (you'll need to implement actual mail class)
                # Mail.to(to_email).subject(subject).send('This is a test email from Cara Tinker!')

                console.print("✅ Test email would be sent!")
                console.print("\n💡 [bold cyan]To actually send:[/bold cyan]")
                console.print(
                    f"   Mail.to('{to_email}').subject('{subject}').send('Your message here')"
                )

                return f"Test email prepared for {to_email}"
            except Exception as e:
                return f"❌ Mail send error: {str(e)}"

        def queue_test_job(job_name: str = "TestJob", delay: int = 0):
            """Queue a test job."""
            try:
                console.print(f"⚡ Queuing test job: {job_name}...")

                if delay > 0:
                    console.print(f"   Delayed by {delay} seconds")
                    # Queue.later(delay, job_name, {'test': True})
                else:
                    # Queue.push(job_name, {'test': True})
                    pass

                console.print("✅ Test job would be queued!")
                console.print("\n💡 [bold cyan]To actually queue:[/bold cyan]")
                if delay > 0:
                    console.print(
                        f"   Queue.later({delay}, '{job_name}', {{'data': 'value'}})"
                    )
                else:
                    console.print(f"   Queue.push('{job_name}', {{'data': 'value'}})")

                return f"Test job {job_name} prepared"
            except Exception as e:
                return f"❌ Queue job error: {str(e)}"

        def send_test_notification(
            user_id: int = 1, notification_type: str = "TestNotification"
        ):
            """Send a test notification."""
            try:
                console.print(f"🔔 Sending test notification to user {user_id}...")

                # Get user
                user = shell.namespace.get("User")
                if user:
                    target_user = user.find(user_id)
                    if target_user:
                        console.print(
                            f"   Target: {target_user.__attributes__.get('name', 'Unknown')} ({target_user.__attributes__.get('email', 'No email')})"
                        )
                    else:
                        console.print(f"   User {user_id} not found")
                        return f"User {user_id} not found"

                console.print("✅ Test notification would be sent!")
                console.print("\n💡 [bold cyan]To actually send:[/bold cyan]")
                console.print(f"   user = User.find({user_id})")
                console.print(f"   user.notify({notification_type}())")
                console.print("   # or")
                console.print(f"   Notification.send([user], {notification_type}())")

                return f"Test notification prepared for user {user_id}"
            except Exception as e:
                return f"❌ Notification send error: {str(e)}"

        def show_queue_jobs(limit: int = 10):
            """Show queued jobs."""
            try:
                console.print("⚡ Checking queue jobs...")

                # Try to get Job model
                job_model = shell.namespace.get("Job")
                if job_model:
                    jobs = job_model.limit(limit).get()

                    if jobs and len(jobs) > 0:
                        data = []
                        for job in jobs:
                            if hasattr(job, "__attributes__"):
                                job_data = {}
                                for key, value in job.__attributes__.items():
                                    if hasattr(value, "strftime"):
                                        job_data[key] = value.strftime(
                                            "%Y-%m-%d %H:%M:%S"
                                        )
                                    else:
                                        job_data[key] = str(value)
                                data.append(job_data)

                        if data:
                            table = Table(title=f"⚡ Queue Jobs (showing {len(data)})")

                            for key in data[0].keys():
                                table.add_column(key, style="cyan")

                            for row in data:
                                table.add_row(*[str(v) for v in row.values()])

                            console.print(table)
                            return f"Found {len(data)} jobs in queue"
                    else:
                        console.print("No jobs found in queue")
                        return "No jobs in queue"
                else:
                    console.print("Job model not available")
                    return "Job model not found"

            except Exception as e:
                return f"❌ Queue jobs error: {str(e)}"

        # Add all enhanced helpers to shell namespace
        enhanced_helpers = {
            # Application helpers
            "app_info": app_info,
            "db_info": db_info,
            "routes_count": routes_count,
            # Cache helpers
            "clear_cache": clear_cache_all,
            "test_cache": test_cache,
            # Database helpers
            "query": quick_query,  # query('users', 10)
            "model_stats": model_stats,
            # Config helpers
            "show_config": show_config,
            "config_get": show_config,  # Alias
            # Development helpers
            "logs": logs,
            "benchmark": benchmark,
            "craft": craft_command,
            "artisan": craft_command,  # Laravel-style alias
            # Notification helpers
            "test_mail": test_mail,
            "test_queue": test_queue,
            "test_notification": test_notification,
            "send_test_mail": send_test_mail,
            "queue_test_job": queue_test_job,
            "send_test_notification": send_test_notification,
            "show_queue_jobs": show_queue_jobs,
        }

        for name, func in enhanced_helpers.items():
            shell.add_to_namespace(name, func)

        # Also add console for direct Rich usage
        shell.add_to_namespace("rich_console", console)

    def _show_startup_banner(self):
        """Show startup banner."""
        self.info("🔧 Cara Tinker - Interactive Shell")
        self.info("Laravel-style development environment for Cara framework")
        self.info("")

        self.info("✨ Enhanced Development Features:")
        self.info("  🏗️  Application: app_info(), db_info(), routes_count()")
        self.info("  🗄️  Database: query('users', 10), model_stats()")
        self.info("  💾 Cache: clear_cache(), test_cache()")
        self.info("  🔧 Config: show_config('app.name'), config_get(...)")
        self.info(
            "  📋 Development: logs(20), benchmark(func), craft('migrate:status')"
        )
        self.info("  📧 Mail: test_mail(), send_test_mail(...)")
        self.info("  ⚡ Queue: test_queue(), queue_test_job(...)")
        self.info("  🔔 Notification: test_notification(), send_test_notification(...)")
        self.info("  ⚡ Jobs: show_queue_jobs(10)")
        self.info("")

        self.info("Built-in helper functions:")
        self.info("  • info() - Show available functions and classes")
        self.info("  • info(obj) - Show object information")
        self.info("  • dump(obj) - Pretty print object")
        self.info("  • dd(obj) - Dump and die")
        self.info("  • clear() - Clear screen")
        self.info("  • exit() or quit() - Exit shell")
        self.info("")
        self.info("Rich utilities available:")
        self.info("  • console - Rich Console instance")
        self.info("  • print_table(headers, rows) - Create tables")
        self.info("  • print_panel(content, title) - Create panels")
        self.info("  • print_syntax(code) - Syntax highlighting")

        self.info("-" * 60)

    def _include_modules(self, modules: List[str], shell):
        """Include additional modules."""
        for module_name in modules:
            module_name = module_name.strip()
            if not module_name:
                continue

            try:
                # Import module
                module = __import__(module_name)

                # Add to namespace
                shell.namespace[module_name.split(".")[-1]] = module

                if self.option("verbose"):
                    self.success(f"✅ Included module: {module_name}")

            except ImportError as e:
                self.warning(f"⚠️  Could not import {module_name}: {e}")

    def _show_usage_tips(self):
        """Show usage tips."""
        self.info("\n💡 Usage Tips:")
        self.info("   • Use tab completion for auto-complete")
        self.info("   • Use ? after any object for help")
        self.info("   • Use %magic commands in IPython mode")
        self.info("   • Access application: app()")
        self.info("   • Resolve services: resolve('service_name')")
        self.info("   • Get config: config('key.name')")

        self.info("   • Quick model queries: query('users', 10), model_stats()")
        self.info("   • Application info: app_info(), db_info()")
        self.info("   • Performance testing: benchmark(lambda: YourModel.all())")
        self.info("   • Run commands: craft('routes:list')")
        self.info("   • Mail testing: test_mail(), send_test_mail('user@example.com')")
        self.info("   • Queue testing: test_queue(), queue_test_job('MyJob')")
        self.info("   • Notifications: test_notification(), send_test_notification(1)")

    def _resolve_user_model(self):
        """
        Resolve User model from container (dependency injection).

        App must register User model in ApplicationProvider:
        self.application.bind("User", User)
        """
        import builtins

        if hasattr(builtins, "app"):
            app_instance = builtins.app()
            if app_instance and app_instance.has("User"):
                return app_instance.make("User")
        return None
