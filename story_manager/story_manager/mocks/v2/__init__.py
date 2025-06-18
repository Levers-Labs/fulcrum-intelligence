"""
V2 Mock Story System

This module provides mock story generation for v2 stories that use patterns and evaluators.
Unlike v1 which uses story groups, v2 generates stories by creating mock pattern results
and then evaluating them through story evaluators.
"""

from .main import MockStoryServiceV2

__all__ = ["MockStoryServiceV2"]
