""">>> help(VectorUtilsMixin)"""

import collections
import numbers

from buzzard._tools import conv

class VectorUtilsMixin(object):
    """Private mixin for the Vector class containing subroutines for fields manipulations"""

    @staticmethod
    def _field_of_def(fielddef):
        """Used on file opening / creation"""
        oft = fielddef.type
        oftstr = conv.str_of_oft(oft)
        type_ = conv.type_of_oftstr(oftstr)
        default = fielddef.GetDefault()
        return {
            'name': fielddef.name,
            'precision': fielddef.precision,
            'width': fielddef.width,
            'nullable': bool(fielddef.IsNullable()),
            'default': None if default is None else type_(default),
            'type': oftstr,
        }

    def _fields_of_lyr(self, lyr):
        """Used on file opening / creation"""
        featdef = lyr.GetLayerDefn()
        field_count = featdef.GetFieldCount()
        return [self._field_of_def(featdef.GetFieldDefn(i)) for i in range(field_count)]

    @staticmethod
    def _normalize_fields_defn(fields):
        """Used on file creation"""
        if not isinstance(fields, collections.Iterable):
            raise TypeError('Bad fields definition type')

        def _sanitize_dict(dic):
            dic = dict(dic)
            name = dic.pop('name')
            type_ = dic.pop('type')
            precision = dic.pop('precision', None)
            width = dic.pop('width', None)
            nullable = dic.pop('nullable', None)
            default = dic.pop('default', None)
            oft = conv.oft_of_any(type_)
            if default is not None:
                default = str(conv.type_of_oftstr(conv.str_of_oft(oft))(default))
            if len(dic) != 0:
                raise ValueError('unexpected keys in {} dict: {}'.format(name, dic))
            return dict(
                name=name,
                type=oft,
                precision=precision,
                width=width,
                nullable=nullable,
                default=default,
            )
        return [_sanitize_dict(dic) for dic in fields]

    def _normalize_field_values(self, fields):
        """Used on feature insertion"""
        if isinstance(fields, collections.Mapping):
            lst = [None] * len(self._fields)
            for k, v in fields.items():
                if v is None:
                    pass
                else:
                    i = self._index_of_field_name[k]
                    lst[i] = self._type_of_field_index[i](v)
            for defn, val in zip(self._fields, lst):
                if val is None and defn['nullable'] is False:
                    raise ValueError('{} not nullable'.format(defn))
            return lst
        elif isinstance(fields, collections.Iterable):
            if len(fields) == 0 and self._all_nullable:
                return [None] * len(self._fields)
            elif len(fields) != len(self._fields):
                raise ValueError('{} fields provided instead of {}'.format(
                    len(fields), len(self._fields),
                ))
            else:
                return [
                    norm(val) if val is not None else None
                    for (norm, val) in zip(self._type_of_field_index, fields)
                ]
        else:
            raise TypeError('Bad fields type')

    def _iter_user_intput_field_keys(self, keys):
        """Used on features reading"""
        if keys == -1:
            for i in range(len(self._fields)):
                yield i
        elif isinstance(keys, str):
            for str_ in keys.replace(' ', ',').split(','):
                if str_ != '':
                    yield self._index_of_field_name[str_]
        elif keys is None:
            return
        elif isinstance(keys, collections.Iterable):
            for val in keys:
                if isinstance(val, numbers.Number):
                    val = int(val)
                    if val >= len(self._fields):
                        raise ValueError('Out of bound %d' % val)
                    yield val
                elif isinstance(val, str):
                    yield self._index_of_field_name[val]
                else:
                    raise TypeError('bad type in `fields`')
        else:
            raise TypeError('bad `fields` type')
