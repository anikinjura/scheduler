# Project Summary

## Overall Goal
Refactor the Ozon parser classes to eliminate code duplication and improve maintainability by moving common functionality to the base class `BaseOzonParser`.

## Key Knowledge
- **Project Structure**: Contains multiple parser classes inheriting from `BaseOzonParser` (e.g., `OzonGiveoutReportParser`, `OzonCarriagesReportParser`)
- **Base Class**: `BaseOzonParser` inherits from `BaseParser` and contains common functionality for Ozon marketplaces
- **Configuration**: Each parser uses configuration files with selectors, URL templates, and report schemas
- **File Locations**: 
  - Base class: `scheduler_runner\tasks\reports\BaseOzonParser.py`
  - Giveout parser: `scheduler_runner\tasks\reports\Parser_KPI_Giveout_OzonScript.py`
  - Carriages parser: `scheduler_runner\tasks\reports\Parser_KPI_Carriages_OzonScript.py`
- **Output**: Reports are saved to `reports\json\` directory in JSON format

## Recent Actions
1. **[DONE]** Removed redundant `_extract_total_giveout()` function from `Parser_KPI_Giveout_OzonScript.py` as it always returned 0
2. **[DONE]** Removed corresponding `TOTAL_GIVEOUT` selector from configuration and `total_packages` field from report schema
3. **[DONE]** Created universal `extract_number_by_selector()` method in `BaseOzonParser` to replace duplicate number extraction logic
4. **[DONE]** Updated `_extract_issued_packages()` to use the universal method
5. **[DONE]** Moved `get_default_selectors()` method to base class
6. **[DONE]** Moved `logout()` method to base class
7. **[DONE]** Moved `login()` method to base class
8. **[DONE]** Created universal `run_parser_with_params()` method in base class to centralize the main execution logic
9. **[DONE]** Updated `Parser_KPI_Giveout_OzonScript.py` to use the new universal method in its `main()` function

## Current Plan
1. **[DONE]** Remove redundant `_extract_total_giveout()` function and related configuration
2. **[DONE]** Implement universal `extract_number_by_selector()` method in base class
3. **[DONE]** Move common methods (`get_default_selectors`, `logout`, `login`) to base class
4. **[DONE]** Create universal `run_parser_with_params()` method in base class
5. **[DONE]** Update `Parser_KPI_Giveout_OzonScript.py` to use universal methods
6. **[TODO]** Apply the same refactoring approach to `Parser_KPI_Carriages_OzonScript.py` to use the universal methods
7. **[TODO]** Consider creating an abstract base method for the main execution flow that can be customized by each parser type

---

## Summary Metadata
**Update time**: 2026-01-08T20:44:12.951Z 
