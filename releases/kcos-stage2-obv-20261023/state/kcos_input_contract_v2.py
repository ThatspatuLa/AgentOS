"""Lossless runtime input views derived from authenticated MQL declarations.

Callers authenticate source bytes and supply semantic runtime bindings. This
module does not infer strategy intent, edit source, choose dates, or invent
optimization domains. Published specifications remain immutable.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


class InputContractError(ValueError):
    pass


INTEGER_BOUNDS = {
    'char': (-128, 127), 'uchar': (0, 255), 'short': (-32768, 32767),
    'ushort': (0, 65535), 'int': (-(2**31), 2**31 - 1),
    'uint': (0, 2**32 - 1), 'long': (-(2**63), 2**63 - 1),
    'ulong': (0, 2**64 - 1),
}
SUPPORTED = set(INTEGER_BOUNDS) | {'bool', 'string', 'float', 'double', 'datetime'}


def _require(condition, message):
    if not condition:
        raise InputContractError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest().upper()


def _without_comments(text):
    """Lex comments without eating quoted URLs or MQL datetime literals."""
    result, index = [], 0
    while index < len(text):
        if text[index] in ('"', "'"):
            quote, begin = text[index], index
            index += 1
            while index < len(text):
                if text[index] == '\\':
                    index += 2
                    continue
                if text[index] == quote:
                    index += 1
                    break
                index += 1
            else:
                raise InputContractError('unterminated source literal')
            result.append(text[begin:index])
        elif text.startswith('//', index):
            end = text.find('\n', index)
            index = len(text) if end < 0 else end
            result.append(' ')
        elif text.startswith('/*', index):
            end = text.find('*/', index + 2)
            _require(end >= 0, 'unterminated source comment')
            result.append('\n' * text[index:end + 2].count('\n') + ' ')
            index = end + 2
        else:
            result.append(text[index])
            index += 1
    return ''.join(result)


def _tokens(text):
    pattern = r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[A-Za-z_][A-Za-z0-9_]*|[^\s]'
    return [(match.group(), match.start(), match.end()) for match in re.finditer(pattern, text)]


def _ambiguous_conditional_offsets(text):
    """Mark unresolved branches; permit conventional whole-header guards."""
    meaningful = [line.strip() for line in text.splitlines() if line.strip()]
    guard = None
    if len(meaningful) >= 2:
        match = re.fullmatch(r'#\s*ifndef\s+([A-Za-z_]\w*)', meaningful[0])
        if match and re.fullmatch(r'#\s*define\s+' + re.escape(match[1]), meaningful[1]):
            guard = match[1]
    active, intervals, offset = [], [], 0
    for line in text.splitlines(keepends=True):
        match = re.match(r'\s*#\s*(if|ifdef|ifndef|elif|else|endif)\b(.*)', line)
        if match:
            directive, rest = match[1], match[2].strip()
            if directive in ('if', 'ifdef', 'ifndef'):
                safe = not active and guard is not None and directive == 'ifndef' and rest == guard
                active.append(not safe)
            elif directive in ('else', 'elif'):
                _require(active, 'unmatched source conditional')
                active[-1] = True
            elif directive == 'endif':
                _require(active, 'unmatched source endif')
                active.pop()
        elif any(active):
            intervals.append((offset, offset + len(line)))
        offset += len(line)
    _require(not active, 'unterminated source conditional')
    return intervals


def _decimal(value):
    _require(not isinstance(value, bool), 'boolean is not a numeric input value')
    _require(isinstance(value, (str, int, float, Decimal)), 'numeric value must be scalar')
    if isinstance(value, str):
        _require(bool(re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?', value.strip())),
                 'malformed numeric value')
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise InputContractError('malformed numeric value') from exc
    _require(number.is_finite(), 'numeric value must be finite')
    return number


def _epoch(value):
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        number = _decimal(value)
        _require(number == number.to_integral_value(), 'datetime requires whole UTC seconds')
        epoch = int(number)
    elif isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r'\d+', text):
            epoch = int(text)
        else:
            if text.startswith("D'") and text.endswith("'"):
                text = text[2:-1]
            parsed = None
            # MQL calendar literals and date-only contract fields explicitly
            # mean UTC here; arbitrary naive ISO timestamps are rejected.
            for fmt in ('%Y.%m.%d', '%Y.%m.%d %H:%M', '%Y.%m.%d %H:%M:%S', '%Y-%m-%d'):
                if not re.fullmatch(r'\d{4}(?:\.\d{2}\.\d{2}(?: \d{2}:\d{2}(?::\d{2})?)?|-\d{2}-\d{2})', text):
                    continue
                try:
                    parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            if parsed is None:
                _require(bool(re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:Z|[+-]\d\d:\d\d)', text)),
                         'datetime requires a calendar date, MQL literal, or explicit UTC offset')
                try:
                    parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
                except ValueError as exc:
                    raise InputContractError('malformed datetime') from exc
            epoch = int(parsed.timestamp())
    else:
        raise InputContractError('unsupported datetime value')
    _require(0 <= epoch <= 32535215999, 'datetime outside MQL supported range')
    return epoch


def coerce_value(value, mql_type):
    """Return one typed runtime value without truncation or nonfinite values."""
    _require(mql_type in SUPPORTED, f'unsupported MQL input type: {mql_type}')
    if mql_type == 'datetime':
        return _epoch(value)
    if mql_type == 'string':
        _require(isinstance(value, str), 'string input requires a string value')
        _require(not any(ch in value for ch in ('\r', '\n', '\0')) and '||' not in value,
                 'string value contains tester field delimiters')
        return value
    if mql_type == 'bool':
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ('true', 'false', '0', '1'):
            return value.strip().lower() in ('true', '1')
        if type(value) is int and value in (0, 1):
            return bool(value)
        raise InputContractError('bool value must be true, false, 0 or 1')
    number = _decimal(value)
    if mql_type in INTEGER_BOUNDS:
        _require(number == number.to_integral_value(), 'fractional value cannot be an integer input')
        lo, hi = INTEGER_BOUNDS[mql_type]
        _require(lo <= number <= hi, f'value outside {mql_type} bounds')
        return int(number)
    result = float(number)
    _require(math.isfinite(result), 'floating input must be finite')
    if mql_type == 'float':
        _require(abs(number) <= Decimal('3.4028234663852886e38'), 'value outside float bounds')
    _require(Decimal(str(result)) == number, 'floating serialization loses significant digits')
    return result


def serialize_value(value, mql_type):
    value = coerce_value(value, mql_type)
    if mql_type == 'bool':
        return 'true' if value else 'false'
    if mql_type == 'string':
        return value
    return str(value)


def _nonnegative_integral_step(value):
    try:
        number = _decimal(value)
    except InputContractError:
        return False
    return number >= 0 and number == number.to_integral_value()


def _source_default(literal, mql_type):
    if mql_type == 'string':
        _require(literal.startswith('"') and literal.endswith('"'), 'source string default is not a literal')
        try:
            literal = json.loads(literal)
        except json.JSONDecodeError as exc:
            raise InputContractError('unsupported source string escape') from exc
    return coerce_value(literal, mql_type)


def discover_inputs(sources):
    """Discover owners/types/defaults; source authentication belongs to caller.

    Unsupported declarations are retained so harmless enum settings outside
    the configured surface do not block discovery. Configuring such a name
    fails closed during reconciliation. Duplicate owners always fail.
    """
    _require(isinstance(sources, dict) and sources, 'input sources must be a nonempty owner/text map')
    inputs, hashes = {}, {}
    for owner, text in sources.items():
        _require(isinstance(owner, str) and owner and isinstance(text, str), 'invalid input source owner/text')
        hashes[owner] = hashlib.sha256(text.encode('utf-8')).hexdigest().upper()
        clean = _without_comments(text)
        conditional_regions = _ambiguous_conditional_offsets(clean)
        tokens = _tokens(clean)
        index = 0
        while index < len(tokens):
            token, begin, end = tokens[index]
            if token not in ('input', 'sinput'):
                index += 1
                continue
            # A quoted "input" is a single token including its quotes.
            kind, cursor = token, index + 1
            _require(cursor < len(tokens), 'incomplete input declaration')
            if tokens[cursor][0] == 'group':
                _require(cursor + 1 < len(tokens) and tokens[cursor + 1][0].startswith('"'), 'invalid input group')
                index = cursor + 2
                continue
            if tokens[cursor][0] == '(':
                cursor += 1
                _require(cursor + 3 < len(tokens) and tokens[cursor][0] == 'name'
                         and tokens[cursor + 1][0] == '=' and tokens[cursor + 2][0].startswith('"')
                         and tokens[cursor + 3][0] == ')', 'unsupported input display-name syntax')
                cursor += 4
            _require(cursor + 2 < len(tokens), 'incomplete input declaration')
            mql_type, name = tokens[cursor][0], tokens[cursor + 1][0]
            _require(re.fullmatch(r'[A-Za-z_]\w*', name) is not None and tokens[cursor + 2][0] == '=',
                     'unsupported input declaration syntax')
            cursor += 3
            value_begin = tokens[cursor][1] if cursor < len(tokens) else len(clean)
            while cursor < len(tokens) and tokens[cursor][0] != ';':
                cursor += 1
            _require(cursor < len(tokens), 'unterminated input declaration')
            literal = clean[value_begin:tokens[cursor][1]].strip()
            _require(name not in inputs, f'duplicate input ownership: {name}')
            _require(not any(lo <= begin < hi for lo, hi in conditional_regions),
                     f'conditional input ownership requires preprocessed source: {name}')
            declaration = dict(owner=owner, mql_type=mql_type, source_default_literal=literal,
                               optimizable=kind == 'input', supported=False, source_default=None)
            # static input is unsupported; it cannot silently become a
            # regular optimizer dimension.
            static = index > 0 and tokens[index - 1][0] == 'static'
            try:
                _require(not static, 'static input declarations require explicit support')
                declaration['source_default'] = _source_default(literal, mql_type)
                declaration['supported'] = True
            except InputContractError as exc:
                declaration['unsupported_reason'] = str(exc)
            inputs[name] = declaration
            index = cursor + 1
    contract = {'schema_version': 1, 'sources': hashes, 'inputs': inputs}
    contract['sha256'] = _sha(contract)
    return contract


def assert_no_shared_input_shadowing(source_text, source_contract):
    """Reject local declarations of names owned by authenticated includes.

    This lexical ownership guard never evaluates candidate defaults, enum
    types or preprocessor branches. Ordinary candidate domains remain the
    caller's responsibility; even an inactive declaration of a shared name
    creates an unsafe ownership ambiguity and is refused.
    """
    _require(isinstance(source_text, str), 'candidate source must be text')
    _require(isinstance(source_contract, dict) and source_contract.get('schema_version') == 1,
             'invalid shared source input contract')
    unsigned = {k: v for k, v in source_contract.items() if k != 'sha256'}
    _require(source_contract.get('sha256') == _sha(unsigned), 'source input contract hash mismatch')
    shared = set(source_contract['inputs'])
    tokens = _tokens(_without_comments(source_text))
    shadows = set()
    for index, (token, _, _) in enumerate(tokens):
        if token not in ('input', 'sinput'):
            continue
        cursor = index + 1
        if cursor >= len(tokens) or tokens[cursor][0] == 'group':
            continue
        if tokens[cursor][0] == '(':
            # Optional display-name metadata contains no declarations; skip
            # balanced parentheses without interpreting the metadata value.
            depth = 1
            cursor += 1
            while cursor < len(tokens) and depth:
                if tokens[cursor][0] == '(':
                    depth += 1
                elif tokens[cursor][0] == ')':
                    depth -= 1
                cursor += 1
            if depth:
                continue
        if cursor + 1 >= len(tokens):
            continue
        mql_type, name = tokens[cursor][0], tokens[cursor + 1][0]
        if re.fullmatch(r'[A-Za-z_]\w*', mql_type) and name in shared:
            shadows.add(name)
    _require(not shadows, 'shared include input redeclared by candidate: ' + ', '.join(sorted(shadows)))


def reconcile_spec(spec, source_contract, bindings=None, *, strict=False):
    """Return effective copied spec and explicit, reproducible provenance.

    ``bindings`` must be authoritative fixed runtime values supplied by the
    caller. Compiled source defaults are reported separately and never
    override the approved experiment's runtime values.
    """
    _require(isinstance(spec, dict), 'spec must be an object')
    _require(isinstance(source_contract, dict) and source_contract.get('schema_version') == 1,
             'invalid source input contract')
    unsigned = {k: v for k, v in source_contract.items() if k != 'sha256'}
    _require(source_contract.get('sha256') == _sha(unsigned), 'source input contract hash mismatch')
    effective = copy.deepcopy(spec)
    config = effective.get('tester_config')
    _require(isinstance(config, dict) and isinstance(config.get('inputs'), dict), 'tester_config.inputs must be an object')
    inputs, declarations = config['inputs'], source_contract['inputs']
    bindings = {} if bindings is None else bindings
    _require(isinstance(bindings, dict), 'runtime bindings must be an object')
    _require(set(bindings) <= set(declarations) and set(bindings) <= set(inputs),
             'runtime binding has no configured source declaration')
    report = {'schema_version': 1, 'contract_sha256': source_contract['sha256'],
              'sources': copy.deepcopy(source_contract['sources']), 'changes': [], 'inputs': {}, 'unresolved_inputs': []}
    if strict:
        _require(set(inputs) <= set(declarations), 'configured inputs missing from complete source contract: '
                 + ', '.join(sorted(set(inputs) - set(declarations))))
    for name, item in inputs.items():
        _require(isinstance(item, dict), f'input config is not an object: {name}')
        declaration = declarations.get(name)
        if declaration is None:
            report['unresolved_inputs'].append(name)
            continue
        _require(declaration.get('supported') is True,
                 f'unsupported source declaration for {name}: {declaration.get("unsupported_reason")}')
        mql_type = declaration['mql_type']
        _require(item.get('optimize') in ('Y', 'N'), f'invalid optimization flag for {name}')
        optimized = item['optimize'] == 'Y'
        _require(not optimized or (declaration['optimizable'] and mql_type not in ('datetime', 'string')),
                 f'input cannot be optimized: {name} ({mql_type})')
        _require(name not in bindings or not optimized, f'fixed runtime binding is optimized: {name}')
        previous = copy.deepcopy(item)
        item['type'] = mql_type
        _require('default' in item, f'input default missing: {name}')
        # Validate supplied fixed values even if an approved binding will
        # replace them; malformed metadata is never quietly erased.
        runtime = coerce_value(item['default'], mql_type)
        if name in bindings:
            runtime = coerce_value(bindings[name], mql_type)
        item['default'] = runtime
        if mql_type in ('datetime', 'string'):
            for field in ('start', 'stop'):
                _require(field in previous, f'input field missing: {name}.{field}')
                parsed = coerce_value(previous[field], mql_type)
                _require(parsed == coerce_value(previous['default'], mql_type),
                         f'fixed input has contradictory range: {name}.{field}')
                item[field] = runtime
            prior_step = previous.get('step')
            if mql_type == 'datetime':
                _require(_nonnegative_integral_step(prior_step) or coerce_value(prior_step, mql_type)
                         == coerce_value(previous['default'], mql_type), f'fixed datetime has ambiguous step: {name}')
                item['step'] = 0
            else:
                _require(_nonnegative_integral_step(prior_step) or prior_step == previous['default'],
                         f'fixed string has ambiguous step: {name}')
                item['step'] = runtime
        else:
            for field in ('start', 'step', 'stop'):
                _require(field in item, f'input field missing: {name}.{field}')
                item[field] = coerce_value(item[field], mql_type)
            if name in bindings:
                item['start'] = item['stop'] = runtime
            if optimized and mql_type != 'bool':
                _require(item['step'] > 0 and item['start'] < item['stop'], f'unusable optimization range: {name}')
            elif optimized:
                _require(item['start'] is False and item['step'] is True and item['stop'] is True,
                         f'unusable boolean optimization range: {name}')
        for field in ('type', 'default', 'start', 'step', 'stop'):
            if previous.get(field) != item[field] or type(previous.get(field)) is not type(item[field]):
                report['changes'].append({'input': name, 'field': field, 'before': previous.get(field),
                                          'after': item[field], 'reason': 'approved_runtime_binding' if name in bindings
                                          and field != 'type' else 'source_type_serialization'})
        report['inputs'][name] = {
            'owner': declaration['owner'], 'mql_type': mql_type,
            'source_default': declaration['source_default'],
            'source_default_literal': declaration['source_default_literal'], 'runtime_default': runtime,
            'runtime_binding_applied': name in bindings,
            'serialization': {field: serialize_value(item[field], mql_type)
                              for field in ('default', 'start', 'step', 'stop')},
        }
    report['unresolved_inputs'].sort()
    return effective, report
