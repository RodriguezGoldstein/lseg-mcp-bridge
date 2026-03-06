from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Iterable, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from auth import BridgeError, SessionManager, get_lseg_module, redact_value

DEFAULT_SEARCH_VIEW = "SEARCH_ALL"
DEFAULT_RIC_VIEW = "EQUITY_QUOTES"
DEFAULT_DISCOVERY_SELECT_FIELDS = [
    "RIC",
    "PrimaryRIC",
    "TickerSymbol",
    "PermID",
    "CompanyName",
    "PrimaryExchange",
    "ExchangeCountry",
]
DEFAULT_SELECT_FIELDS = list(DEFAULT_DISCOVERY_SELECT_FIELDS)
DEFAULT_RIC_SELECT_FIELDS = [
    "RIC",
    "PrimaryRIC",
    "TickerSymbol",
    "CommonName",
    "ExchangeName",
    "ExchangeCode",
    "ExchangeCountry",
    "AssetState",
]
MAX_SEARCH_TOP = 250
DEFAULT_REGION_TOP = 25
DEFAULT_COMPANY_TOP = 25
DEFAULT_RIC_TOP = 100


class MetadataPropertyDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    path: str
    parent_path: str | None = None
    property_type: str = Field(alias="type")
    searchable: bool = False
    sortable: bool = False
    navigable: bool = False
    groupable: bool = False
    exact: bool = False
    symbol: bool = False
    has_nested_properties: bool = False
    depth: int = 1


class SearchMetadataResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    view: str
    property_count: int
    properties: list[MetadataPropertyDefinition]
    searchable_properties: list[str]
    sortable_properties: list[str]
    navigable_properties: list[str]
    groupable_properties: list[str]
    exact_properties: list[str]
    symbol_properties: list[str]


class SearchRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ric: str | None = None
    primary_ric: str | None = None
    ticker_symbol: str | None = None
    perm_id: str | None = None
    company_name: str | None = None
    primary_exchange: str | None = None
    exchange_country: str | None = None
    common_name: str | None = None
    exchange_name: str | None = None
    exchange_code: str | None = None
    asset_state: str | None = None
    requested_region: str | None = None
    request_context: dict[str, Any] = Field(default_factory=dict)
    attributes: dict[str, Any] = Field(default_factory=dict)


class LsegSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    view: str = DEFAULT_SEARCH_VIEW
    filter: str | None = None
    select_fields: list[str] = Field(default_factory=lambda: list(DEFAULT_DISCOVERY_SELECT_FIELDS))
    order_by: str | None = None
    top: int = Field(default=DEFAULT_REGION_TOP, ge=1, le=MAX_SEARCH_TOP)
    skip: int = Field(default=0, ge=0)

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("query must be a non-empty string")
        return stripped

    @field_validator("view", mode="before")
    @classmethod
    def _validate_view(cls, value: Any) -> str:
        return resolve_search_view(value)

    @field_validator("select_fields", mode="before")
    @classmethod
    def _validate_select_fields(cls, value: Any) -> list[str]:
        return _normalize_select_fields(value, default_fields=DEFAULT_DISCOVERY_SELECT_FIELDS)


class LsegSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    view: str
    filter: str | None = None
    select_fields: list[str]
    order_by: str | None = None
    top: int
    total: int | None = None
    row_count: int
    records: list[SearchRecord]


class RegionalSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    regions: list[str]
    view: str = DEFAULT_SEARCH_VIEW
    select_fields: list[str] = Field(default_factory=lambda: list(DEFAULT_DISCOVERY_SELECT_FIELDS))
    top_per_region: int = Field(default=DEFAULT_REGION_TOP, ge=1, le=MAX_SEARCH_TOP)
    additional_filter: str | None = None
    order_by: str | None = None

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("query must be a non-empty string")
        return stripped

    @field_validator("regions", mode="before")
    @classmethod
    def _validate_regions(cls, value: Any) -> list[str]:
        if isinstance(value, str):
            parsed = [item.strip() for item in value.split(",")]
        else:
            parsed = [str(item).strip() for item in value]
        regions = [item for item in parsed if item]
        if not regions:
            raise ValueError("regions must include at least one non-empty region")
        return _dedupe_preserving_order(regions)

    @field_validator("view", mode="before")
    @classmethod
    def _validate_view(cls, value: Any) -> str:
        return resolve_search_view(value)

    @field_validator("select_fields", mode="before")
    @classmethod
    def _validate_select_fields(cls, value: Any) -> list[str]:
        return _normalize_select_fields(value, default_fields=DEFAULT_DISCOVERY_SELECT_FIELDS)


class RegionalSearchBucket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    region: str
    filter: str
    row_count: int
    records: list[SearchRecord]


class RegionalSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: RegionalSearchRequest
    total_row_count: int
    region_results: list[RegionalSearchBucket]
    records: list[SearchRecord]


class CompanyLookupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str | None = None
    sedol: str | None = None
    exchange_country: str | None = None
    exchange_code: str | None = None
    ric: str | None = None
    common_name: str | None = None
    name: str | None = None
    view: str = DEFAULT_SEARCH_VIEW
    select_fields: list[str] = Field(default_factory=lambda: list(DEFAULT_DISCOVERY_SELECT_FIELDS))
    top: int = Field(default=DEFAULT_COMPANY_TOP, ge=1, le=MAX_SEARCH_TOP)

    @field_validator("view", mode="before")
    @classmethod
    def _validate_view(cls, value: Any) -> str:
        return resolve_search_view(value)

    @field_validator("select_fields", mode="before")
    @classmethod
    def _validate_select_fields(cls, value: Any) -> list[str]:
        return _normalize_select_fields(value, default_fields=DEFAULT_DISCOVERY_SELECT_FIELDS)

    @model_validator(mode="after")
    def _validate_identifiers(self) -> "CompanyLookupRequest":
        query_identifiers = [
            self.ticker,
            self.sedol,
            self.ric,
            self.name,
            self.common_name,
        ]
        if not any(item and item.strip() for item in query_identifiers if isinstance(item, str)):
            raise ValueError(
                "at least one query-capable company identifier must be provided "
                "(ticker, sedol, ric, name, or common_name)"
            )
        return self


class CompanyLookupResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: CompanyLookupRequest
    query: str
    filter: str
    row_count: int
    records: list[SearchRecord]


class CompanyLookupBatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_count: int
    total_row_count: int
    results: list[CompanyLookupResult]


class RicLookupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str
    exchange_country: str | None = None
    exchange_code: str | None = None
    view: str = DEFAULT_RIC_VIEW
    select_fields: list[str] = Field(default_factory=lambda: list(DEFAULT_RIC_SELECT_FIELDS))
    top: int = Field(default=DEFAULT_RIC_TOP, ge=1, le=MAX_SEARCH_TOP)
    order_by: str = "ExchangeName asc"

    @field_validator("ticker")
    @classmethod
    def _validate_ticker(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("ticker must be a non-empty string")
        return stripped

    @field_validator("view", mode="before")
    @classmethod
    def _validate_view(cls, value: Any) -> str:
        return resolve_search_view(value)

    @field_validator("select_fields", mode="before")
    @classmethod
    def _validate_select_fields(cls, value: Any) -> list[str]:
        return _normalize_select_fields(value, default_fields=DEFAULT_RIC_SELECT_FIELDS)

    @model_validator(mode="after")
    def _validate_market_context(self) -> "RicLookupRequest":
        if not (self.exchange_country or self.exchange_code):
            raise ValueError("exchange_country or exchange_code must be provided")
        return self


class RicLookupResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: RicLookupRequest
    query: str
    filter: str
    row_count: int
    resolved_rics: list[str]
    records: list[SearchRecord]


def get_search_metadata(
    *,
    view: Any = DEFAULT_SEARCH_VIEW,
    property_name: str | None = None,
    searchable: bool | None = None,
    sortable: bool | None = None,
    navigable: bool | None = None,
    groupable: bool | None = None,
    exact: bool | None = None,
    symbol: bool | None = None,
) -> SearchMetadataResult:
    """Fetch metadata definitions and optionally filter the property set."""
    resolved_view = resolve_search_view(view)
    metadata = _get_search_metadata_cached(resolved_view).model_copy(deep=True)
    properties = metadata.properties

    if property_name:
        property_key = property_name.strip().lower()
        exact_matches = [item for item in properties if item.path.lower() == property_key]
        if exact_matches:
            properties = exact_matches
        else:
            if property_key in _get_ambiguous_property_names(resolved_view):
                raise BridgeError(
                    code="validation_failed",
                    message=(
                        f"Search property '{property_name}' is ambiguous for view '{resolved_view}'. "
                        "Use the fully qualified property path."
                    ),
                    details={"view": resolved_view, "property_name": property_name},
                )
            properties = [
                item
                for item in properties
                if item.name.lower() == property_key
            ]
        if not properties:
            raise BridgeError(
                code="validation_failed",
                message=f"Unsupported search property '{property_name}' for view '{resolved_view}'.",
                details={"view": resolved_view, "property_name": property_name},
            )

    properties = filter_properties_by_attributes(
        properties,
        searchable=searchable,
        sortable=sortable,
        navigable=navigable,
        groupable=groupable,
        exact=exact,
        symbol=symbol,
    )
    return _build_metadata_result(resolved_view, properties)


def list_searchable_properties(*, view: Any = DEFAULT_SEARCH_VIEW) -> list[str]:
    return get_search_metadata(view=view, searchable=True).searchable_properties


def list_sortable_properties(*, view: Any = DEFAULT_SEARCH_VIEW) -> list[str]:
    return get_search_metadata(view=view, sortable=True).sortable_properties


def get_property_definition(property_name: str, *, view: Any = DEFAULT_SEARCH_VIEW) -> MetadataPropertyDefinition:
    metadata = get_search_metadata(view=view, property_name=property_name)
    return metadata.properties[0]


def filter_properties_by_attributes(
    properties: Sequence[MetadataPropertyDefinition],
    *,
    searchable: bool | None = None,
    sortable: bool | None = None,
    navigable: bool | None = None,
    groupable: bool | None = None,
    exact: bool | None = None,
    symbol: bool | None = None,
) -> list[MetadataPropertyDefinition]:
    filtered = list(properties)
    flag_filters = {
        "searchable": searchable,
        "sortable": sortable,
        "navigable": navigable,
        "groupable": groupable,
        "exact": exact,
        "symbol": symbol,
    }
    for attribute_name, expected in flag_filters.items():
        if expected is None:
            continue
        filtered = [item for item in filtered if getattr(item, attribute_name) is expected]
    return filtered


def execute_search(request: LsegSearchRequest | Mapping[str, Any]) -> LsegSearchResult:
    """Execute a content search request and normalize hits into stable records."""
    typed_request = request if isinstance(request, LsegSearchRequest) else _validate_model(
        LsegSearchRequest,
        request,
    )
    search_module = _get_search_module()
    snapshot = SessionManager.instance().ensure_session()
    select_fields = _validate_property_selection(typed_request.select_fields, view=typed_request.view)
    _validate_order_by(typed_request.order_by, view=typed_request.view)

    definition = search_module.Definition(
        query=typed_request.query,
        view=_get_search_view_enum(typed_request.view),
        filter=typed_request.filter,
        order_by=typed_request.order_by,
        select=",".join(select_fields),
        top=typed_request.top,
        skip=typed_request.skip,
    )
    response = definition.get_data(session=snapshot.session)
    _ensure_success(response, source="search.Definition")

    raw = getattr(response.data, "raw", {}) or {}
    records = [
        _normalize_search_hit(hit, request_context={})
        for hit in _extract_hits(raw)
    ]
    total = _normalize_total(raw)
    return LsegSearchResult(
        query=typed_request.query,
        view=typed_request.view,
        filter=typed_request.filter,
        select_fields=select_fields,
        order_by=typed_request.order_by,
        top=typed_request.top,
        total=total,
        row_count=len(records),
        records=records,
    )


def search_by_region(request: RegionalSearchRequest | Mapping[str, Any]) -> RegionalSearchResult:
    """Search a topic/company/subject across multiple regions and accumulate results."""
    typed_request = request if isinstance(request, RegionalSearchRequest) else _validate_model(
        RegionalSearchRequest,
        request,
    )
    aggregated_records: list[SearchRecord] = []
    region_results: list[RegionalSearchBucket] = []

    for region in typed_request.regions:
        combined_filter = _combine_filters(
            [
                typed_request.additional_filter,
                _equals_filter("ExchangeCountry", region),
                "TickerSymbol ne null",
                "PermID ne null",
            ]
        )
        result = execute_search(
            LsegSearchRequest(
                query=typed_request.query,
                view=typed_request.view,
                filter=combined_filter,
                select_fields=typed_request.select_fields,
                order_by=typed_request.order_by,
                top=typed_request.top_per_region,
            )
        )
        region_records = [
            record.model_copy(
                update={
                    "request_context": _normalize_json_value(
                        {
                            **record.request_context,
                            "region": region,
                            "query": typed_request.query,
                        }
                    ),
                    "requested_region": region,
                }
            )
            for record in result.records
        ]
        aggregated_records.extend(region_records)
        region_results.append(
            RegionalSearchBucket(
                region=region,
                filter=combined_filter,
                row_count=len(region_records),
                records=region_records,
            )
        )

    return RegionalSearchResult(
        request=typed_request,
        total_row_count=len(aggregated_records),
        region_results=region_results,
        records=aggregated_records,
    )


def company_lookup(
    requests: CompanyLookupRequest | Mapping[str, Any] | Sequence[CompanyLookupRequest | Mapping[str, Any]],
) -> CompanyLookupBatchResult:
    """Resolve companies using one or more precise identifier combinations."""
    if isinstance(requests, (CompanyLookupRequest, Mapping)):
        request_items = [requests]
    else:
        if isinstance(requests, (str, bytes, bytearray)):
            raise BridgeError(
                code="validation_failed",
                message="company_lookup requests must be a mapping or a sequence of mappings.",
            )
        request_items = list(requests)

    typed_requests = [
        item if isinstance(item, CompanyLookupRequest) else _validate_model(CompanyLookupRequest, item)
        for item in request_items
    ]

    results: list[CompanyLookupResult] = []
    total_row_count = 0
    for request_item in typed_requests:
        query = _select_company_query(request_item)
        filter_expression = _build_company_lookup_filter(request_item)

        # Combining multiple identifiers here improves precision because the search
        # query seeds the candidate set while the structured filter narrows it to a
        # specific listing or issuer context.
        search_result = execute_search(
            LsegSearchRequest(
                query=query,
                view=request_item.view,
                filter=filter_expression,
                select_fields=request_item.select_fields,
                top=request_item.top,
            )
        )
        request_context = {
            "ticker": request_item.ticker,
            "sedol": request_item.sedol,
            "exchange_country": request_item.exchange_country,
            "exchange_code": request_item.exchange_code,
            "ric": request_item.ric,
            "name": request_item.name,
            "common_name": request_item.common_name,
        }
        records = [
            record.model_copy(
                update={
                    "request_context": _normalize_json_value(
                        {**record.request_context, **request_context}
                    )
                }
            )
            for record in search_result.records
        ]
        results.append(
            CompanyLookupResult(
                request=request_item,
                query=query,
                filter=filter_expression,
                row_count=len(records),
                records=records,
            )
        )
        total_row_count += len(records)

    return CompanyLookupBatchResult(
        request_count=len(typed_requests),
        total_row_count=total_row_count,
        results=results,
    )


def lookup_companies(
    requests: CompanyLookupRequest | Mapping[str, Any] | Sequence[CompanyLookupRequest | Mapping[str, Any]],
) -> CompanyLookupBatchResult:
    """Backward-compatible alias for company_lookup()."""
    return company_lookup(requests)


def lookup_ric(request: RicLookupRequest | Mapping[str, Any]) -> RicLookupResult:
    """Resolve exchange-contextual RIC candidates for a ticker."""
    typed_request = request if isinstance(request, RicLookupRequest) else _validate_model(
        RicLookupRequest,
        request,
    )
    filter_expression = _build_ric_lookup_filter(typed_request)
    search_result = execute_search(
        LsegSearchRequest(
            query=typed_request.ticker,
            view=typed_request.view,
            filter=filter_expression,
            select_fields=typed_request.select_fields,
            top=typed_request.top,
            order_by=typed_request.order_by,
        )
    )
    records = [
        record.model_copy(
            update={
                "request_context": _normalize_json_value(
                    {
                        **record.request_context,
                        "ticker": typed_request.ticker,
                        "exchange_country": typed_request.exchange_country,
                        "exchange_code": typed_request.exchange_code,
                    }
                )
            }
        )
        for record in search_result.records
    ]
    resolved_rics = _dedupe_preserving_order(
        [record.ric for record in records if record.ric]
    )
    return RicLookupResult(
        request=typed_request,
        query=typed_request.ticker,
        filter=filter_expression,
        row_count=len(records),
        resolved_rics=resolved_rics,
        records=records,
    )


def resolve_search_view(view: Any) -> str:
    """Accept search enums or strings and normalize them to search.Views member names."""
    search_module = _get_search_module()
    if isinstance(view, str):
        candidate = view.strip()
    elif isinstance(view, Enum):
        candidate = view.name if hasattr(view, "name") else str(view.value)
    elif hasattr(view, "name"):
        candidate = str(view.name)
    elif hasattr(view, "value"):
        candidate = str(view.value)
    else:
        raise BridgeError(
            code="validation_failed",
            message=f"Unsupported search view value: {view!r}",
            details={"view": redact_value(repr(view))},
        )

    if not candidate:
        raise BridgeError(
            code="validation_failed",
            message="Search view must be a non-empty string or enum value.",
        )

    if candidate in search_module.Views.__members__:
        return candidate

    candidate_lower = candidate.lower()
    for enum_item in search_module.Views:
        if candidate_lower in {enum_item.name.lower(), str(enum_item.value).lower()}:
            return enum_item.name

    raise BridgeError(
        code="validation_failed",
        message=f"Unsupported search view '{candidate}'.",
        details={"view": candidate},
    )


@lru_cache(maxsize=16)
def _get_search_metadata_cached(view: str) -> SearchMetadataResult:
    search_module = _get_search_module()
    snapshot = SessionManager.instance().ensure_session()
    response = search_module.metadata.Definition(view=_get_search_view_enum(view)).get_data(session=snapshot.session)
    _ensure_success(response, source="search.metadata.Definition")

    raw = getattr(response.data, "raw", {}) or {}
    property_definitions = _flatten_metadata_properties(raw.get("Properties", {}))
    return _build_metadata_result(view, property_definitions)


@lru_cache(maxsize=16)
def _get_property_index(view: str) -> dict[str, MetadataPropertyDefinition]:
    metadata = _get_search_metadata_cached(view)
    ambiguous_names = _get_ambiguous_property_names(view)
    property_index: dict[str, MetadataPropertyDefinition] = {}
    for property_definition in metadata.properties:
        property_index[property_definition.path.lower()] = property_definition
        short_name = property_definition.name.lower()
        if short_name not in ambiguous_names:
            property_index[short_name] = property_definition
    return property_index


@lru_cache(maxsize=16)
def _get_ambiguous_property_names(view: str) -> frozenset[str]:
    metadata = _get_search_metadata_cached(view)
    counts: dict[str, int] = {}
    for property_definition in metadata.properties:
        short_name = property_definition.name.lower()
        counts[short_name] = counts.get(short_name, 0) + 1
    return frozenset(name for name, count in counts.items() if count > 1)


def _build_metadata_result(view: str, properties: Sequence[MetadataPropertyDefinition]) -> SearchMetadataResult:
    property_names = [item.path for item in properties]
    return SearchMetadataResult(
        view=view,
        property_count=len(properties),
        properties=list(properties),
        searchable_properties=[item.path for item in properties if item.searchable],
        sortable_properties=[item.path for item in properties if item.sortable],
        navigable_properties=[item.path for item in properties if item.navigable],
        groupable_properties=[item.path for item in properties if item.groupable],
        exact_properties=[item.path for item in properties if item.exact],
        symbol_properties=[item.path for item in properties if item.symbol],
    )


def _flatten_metadata_properties(
    raw_properties: Mapping[str, Any],
    *,
    parent_path: tuple[str, ...] = (),
) -> list[MetadataPropertyDefinition]:
    items: list[MetadataPropertyDefinition] = []
    for property_name, property_data in raw_properties.items():
        path_parts = (*parent_path, property_name)
        nested_properties = property_data.get("Properties")
        path = ".".join(path_parts)
        items.append(
            MetadataPropertyDefinition(
                name=property_name,
                path=path,
                parent_path=".".join(parent_path) if parent_path else None,
                type=str(property_data.get("Type", "")),
                searchable=bool(property_data.get("Searchable", False)),
                sortable=bool(property_data.get("Sortable", False)),
                navigable=bool(property_data.get("Navigable", False)),
                groupable=bool(property_data.get("Groupable", False)),
                exact=bool(property_data.get("Exact", False)),
                symbol=bool(property_data.get("Symbol", False)),
                has_nested_properties=bool(isinstance(nested_properties, Mapping) and nested_properties),
                depth=len(path_parts),
            )
        )
        if isinstance(nested_properties, Mapping):
            items.extend(_flatten_metadata_properties(nested_properties, parent_path=path_parts))
    return items


def _build_company_lookup_filter(request: CompanyLookupRequest) -> str:
    filters = [
        _equals_filter("TickerSymbol", request.ticker) if request.ticker else None,
        _equals_filter("SEDOL", request.sedol) if request.sedol else None,
        _equals_filter("ExchangeCountry", request.exchange_country) if request.exchange_country else None,
        _equals_filter("ExchangeCode", request.exchange_code) if request.exchange_code else None,
        _equals_filter("RIC", request.ric) if request.ric else None,
        _equals_filter("CommonName", request.common_name) if request.common_name else None,
        _equals_filter("CompanyName", request.name) if request.name else None,
    ]
    return _combine_filters(filters)


def _build_ric_lookup_filter(request: RicLookupRequest) -> str:
    filters = [
        _equals_filter("TickerSymbol", request.ticker),
        _equals_filter("ExchangeCountry", request.exchange_country) if request.exchange_country else None,
        _equals_filter("ExchangeCode", request.exchange_code) if request.exchange_code else None,
    ]
    return _combine_filters(filters)


def _select_company_query(request: CompanyLookupRequest) -> str:
    for candidate in (request.ticker, request.name, request.common_name, request.ric, request.sedol):
        if candidate and candidate.strip():
            return candidate.strip()
    raise BridgeError(
        code="validation_failed",
        message="Unable to build company lookup query because no usable identifier was provided.",
    )


def _ensure_success(response: Any, *, source: str) -> None:
    if getattr(response, "is_success", True):
        return
    errors = [
        {
            "code": getattr(error, "code", None),
            "message": getattr(error, "message", None),
        }
        for error in getattr(response, "errors", []) or []
    ]
    raise BridgeError(
        code="data_request_failed",
        message=f"LSEG content search request failed for {source}.",
        details={"source": source, "errors": redact_value(errors)},
    )


def _extract_hits(raw: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    hits = raw.get("Hits", [])
    return [item for item in hits if isinstance(item, Mapping)]


def _normalize_total(raw: Mapping[str, Any]) -> int | None:
    total = raw.get("Total")
    if isinstance(total, int):
        return total
    if isinstance(total, str) and total.isdigit():
        return int(total)
    return None


def _normalize_search_hit(hit: Mapping[str, Any], *, request_context: Mapping[str, Any]) -> SearchRecord:
    field_map = {
        "RIC": "ric",
        "PrimaryRIC": "primary_ric",
        "TickerSymbol": "ticker_symbol",
        "PermID": "perm_id",
        "CompanyName": "company_name",
        "PrimaryExchange": "primary_exchange",
        "ExchangeCountry": "exchange_country",
        "CommonName": "common_name",
        "ExchangeName": "exchange_name",
        "ExchangeCode": "exchange_code",
        "AssetState": "asset_state",
    }

    normalized: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    for key, value in hit.items():
        normalized_key = field_map.get(key)
        json_safe_value = _normalize_json_value(value)
        if normalized_key:
            normalized[normalized_key] = json_safe_value
        else:
            attributes[key] = json_safe_value

    return SearchRecord(
        **normalized,
        requested_region=_normalize_json_value(request_context.get("region")),
        request_context=_normalize_json_value(dict(request_context)),
        attributes=attributes,
    )


def _normalize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return redact_value(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_normalize_json_value(item) for item in value]
    return redact_value(repr(value))


def _combine_filters(filters: Iterable[str | None]) -> str:
    filtered = [item.strip() for item in filters if item and item.strip()]
    if not filtered:
        raise BridgeError(
            code="validation_failed",
            message="At least one valid search filter is required.",
        )
    return " and ".join(filtered)


def _equals_filter(field_name: str, value: str) -> str:
    return f"{field_name} eq '{_escape_literal(value)}'"


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _normalize_select_fields(value: Any, *, default_fields: Sequence[str]) -> list[str]:
    if value is None:
        parsed = list(default_fields)
    elif isinstance(value, str):
        parsed = [field.strip() for field in value.split(",")]
    else:
        parsed = [str(field).strip() for field in value]
    fields = [field for field in parsed if field]
    if not fields:
        raise ValueError("select_fields must contain at least one field")
    return _dedupe_preserving_order(fields)


def _validate_model(model_type: type[BaseModel], value: Any) -> Any:
    try:
        return model_type.model_validate(value)
    except ValidationError as exc:
        raise BridgeError.from_exception(
            "validation_failed",
            exc,
            details={"model": model_type.__name__},
        ) from exc


def _validate_property_selection(select_fields: Sequence[str], *, view: str) -> list[str]:
    property_index = _get_property_index(view)
    ambiguous_names = _get_ambiguous_property_names(view)
    validated: list[str] = []
    unsupported: list[str] = []
    ambiguous: list[str] = []
    for field_name in select_fields:
        normalized_field_name = field_name.lower()
        if "." not in field_name and normalized_field_name in ambiguous_names:
            ambiguous.append(field_name)
            continue
        property_definition = property_index.get(normalized_field_name)
        if property_definition is None:
            unsupported.append(field_name)
            continue
        validated.append(property_definition.path)
    if unsupported or ambiguous:
        raise BridgeError(
            code="validation_failed",
            message="One or more search select fields are not supported for the requested view.",
            details={
                "view": view,
                "unsupported_fields": unsupported,
                "ambiguous_fields": ambiguous,
                "available_field_count": len(_get_search_metadata_cached(view).properties),
            },
        )
    return _dedupe_preserving_order(validated)


def _validate_order_by(order_by: str | None, *, view: str) -> None:
    if not order_by:
        return
    property_index = _get_property_index(view)
    ambiguous_names = _get_ambiguous_property_names(view)
    invalid_fields: list[str] = []
    non_sortable_fields: list[str] = []
    ambiguous_fields: list[str] = []
    for clause in [item.strip() for item in order_by.split(",") if item.strip()]:
        field_name = clause.split()[0]
        normalized_field_name = field_name.lower()
        if "." not in field_name and normalized_field_name in ambiguous_names:
            ambiguous_fields.append(field_name)
            continue
        property_definition = property_index.get(normalized_field_name)
        if property_definition is None:
            invalid_fields.append(field_name)
            continue
        if not property_definition.sortable:
            non_sortable_fields.append(property_definition.path)
    if invalid_fields or non_sortable_fields:
        raise BridgeError(
            code="validation_failed",
            message="The order_by clause contains unsupported or non-sortable properties.",
            details={
                "view": view,
                "invalid_fields": invalid_fields,
                "ambiguous_fields": ambiguous_fields,
                "non_sortable_fields": non_sortable_fields,
            },
        )


def _dedupe_preserving_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _get_search_view_enum(view: str) -> Any:
    search_module = _get_search_module()
    return search_module.Views[view]


def _get_search_module() -> Any:
    get_lseg_module()
    from lseg.data.content import search

    return search
