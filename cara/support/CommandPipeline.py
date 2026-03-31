"""
Command Pipeline for workflow execution.
"""

from loguru import logger


class CommandPipeline:
    """Pipeline for executing commands in sequence."""
    
    def __init__(self, application):
        """Initialize pipeline with application instance."""
        self.application = application
        self.commands = []
        self.context = {}
    
    def pipe(self, command):
        """Add a command to the pipeline."""
        self.commands.append(command)
        return self
    
    def with_context(self, **context):
        """Set context for pipeline execution."""
        self.context.update(context)
        return self
    
    async def send(self, passable=None):
        """Execute the pipeline."""
        bound_logger = logger.bind(service_name="Library", module="CommandPipeline")
        
        if passable is not None:
            self.context.update(passable)
            
        bound_logger.info(f"Starting pipeline with {len(self.commands)} commands")
        
        current_context = self.context.copy()
        
        for i, command in enumerate(self.commands):
            try:
                bound_logger.info(f"Executing command {i+1}/{len(self.commands)}: {command.__name__}")
                
                # Execute command with context
                if hasattr(command, 'handle'):
                    result = await command.handle(current_context, self._next_step)
                    if result:
                        current_context.update(result)
                else:
                    bound_logger.warning(f"Command {command.__name__} has no handle method")
                    
            except Exception as e:
                bound_logger.error(f"Command {command.__name__} failed: {str(e)}")
                current_context['pipeline_error'] = str(e)
                current_context['pipeline_success'] = False
                break
        else:
            current_context['pipeline_success'] = True
            
        bound_logger.info("Pipeline execution completed")
        return current_context
    
    async def _next_step(self, context):
        """Continue to next step in pipeline."""
        return context


def workflow(application):
    """Create a new workflow pipeline."""
    return CommandPipeline(application)
