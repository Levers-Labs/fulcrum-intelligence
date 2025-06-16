"""
V2 Mock Stories Package

This package provides a comprehensive mock story generation system for v2 stories
using a pattern-based approach.

Structure:
- generators/patterns/: Pattern result generators for each pattern type
- generators/stories/: Story generators that use evaluators to create stories from patterns
- services/: High-level services for convenient story generation

Usage:
    from story_manager.mocks.v2 import V2MockStoryService
    from levers.models import Granularity

    service = V2MockStoryService()
    metric = service.get_sample_metric()
    stories = await service.generate_all_pattern_stories(metric, Granularity.WEEK)
"""

# Pattern generators
from .generators.patterns import (
    DimensionAnalysisPatternGenerator,
    HistoricalPerformancePatternGenerator,
    PatternResultGeneratorBase,
    PerformanceStatusPatternGenerator,
)

# Story generators
from .generators.stories import (
    DimensionAnalysisStoryGenerator,
    HistoricalPerformanceStoryGenerator,
    PerformanceStatusStoryGenerator,
    StoryGeneratorBase,
)

# Services
from .services import V2MockStoryService

__all__ = [
    # Pattern result generators
    "PatternResultGeneratorBase",
    "DimensionAnalysisPatternGenerator",
    "HistoricalPerformancePatternGenerator",
    "PerformanceStatusPatternGenerator",
    # Story generators
    "StoryGeneratorBase",
    "DimensionAnalysisStoryGenerator",
    "HistoricalPerformanceStoryGenerator",
    "PerformanceStatusStoryGenerator",
    # Services
    "V2MockStoryService",
]
