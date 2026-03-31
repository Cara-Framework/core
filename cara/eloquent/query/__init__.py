# Query Package - Clean separation of query building concerns
"""
Query Package

This package provides a clean, SOLID-principle based query building system.
Each component has a single responsibility and is easily extensible.
"""

from .builders.AggregateBuilder import AggregateBuilder
from .builders.DeleteBuilder import DeleteBuilder
from .builders.InsertBuilder import InsertBuilder
from .builders.JoinBuilder import JoinBuilder
# Query builders by responsibility
from .builders.SelectBuilder import SelectBuilder
from .builders.UpdateBuilder import UpdateBuilder
from .builders.WhereBuilder import WhereBuilder
from .components.GroupByClause import GroupByClause
from .components.JoinClause import JoinClause
from .components.OrderByClause import OrderByClause
# Query components
from .components.WhereClause import WhereClause
# Relationships
from .EagerRelation import EagerRelations
# Core query builder
from .QueryBuilder import QueryBuilder

__all__ = [
    'QueryBuilder',
    'SelectBuilder',
    'WhereBuilder', 
    'JoinBuilder',
    'AggregateBuilder',
    'UpdateBuilder',
    'InsertBuilder',
    'DeleteBuilder',
    'WhereClause',
    'JoinClause', 
    'OrderByClause',
    'GroupByClause',
    'EagerRelations',
]
