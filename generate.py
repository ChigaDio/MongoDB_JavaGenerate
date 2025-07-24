import json
import uuid

def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def generate_class_name(type_name):
    components = type_name.split('_')
    camel_case = ''.join(x[0].upper() + x[1:] for x in components)
    return f"{camel_case}CollectionData"

def generate_java_type(var_type):
    """Convert variable type to Java type."""
    type_map = {"string": "String", "int": "Integer", "bool": "Boolean", "double": "Double"}
    return type_map.get(var_type, var_type)

def generate_field_declaration(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"    private {var_type} {var_name}; // {column['variable_explanation']}"

def generate_getter(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    camel_name = to_camel_case(var_name)
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"""
    public {var_type} get{camel_name[0].upper() + camel_name[1:]}() {{
        return {var_name};
    }}"""

def generate_setter(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    camel_name = to_camel_case(var_name)
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"""
    public void set{camel_name[0].upper() + camel_name[1:]}({var_type} {var_name}) {{
        this.{var_name} = {var_name};
    }}"""

def generate_query_methods(query, collection_name, class_name, columns):
    method_name = query["method_name"]
    where_conditions = query.get("where", [])
    query_type = query["type"]
    methods = []
    
    return_type_single = f"DataBaseResultPair<Boolean, {class_name}>"
    return_type_many = f"DataBaseResultPair<Boolean, List<{class_name}>>"
    
    # Find unique field for cache updates
    unique_field = next((col["variable_name"] for col in columns if col["index_type"] == "unique"), None)
    unique_field_getter = f"get{to_camel_case(unique_field)[0].upper() + to_camel_case(unique_field)[1:]}" if unique_field else None
    
    # Handle sorting and limit
    sort_str = ""
    limit_str = ""
    is_limit_one = False
    if "order" in query:
        sorts = query["order"].get("sort", [])
        sort_clauses = [f"{('ascending' if s['type'] == 'asc' else 'descending')}(\"{s['comparison']}\")" for s in sorts]
        if sort_clauses:
            sort_str = f".sort({', '.join(sort_clauses)})"
        if "order" in query and query["order"]["limit"] == 1:
            limit_str = ".limit(1)"
            is_limit_one = True
    
    # Generate getter name for nested fields
    def get_getter_name(field, prefix="item"):
        if "." in field:
            parts = field.split(".")
            base = to_camel_case(parts[0])
            nested = to_camel_case(parts[1])
            return f"{prefix}.get{base[0].upper() + base[1:]}().get{nested[0].upper() + nested[1:]}()"
        return f"{prefix}.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}()"
    
    # Generate filter clause and parameter variations for WHERE conditions
    param_variations = []
    filter_conditions = []
    for condition in where_conditions:
        field = condition["comparison"]
        param_name = f"where_{field.replace('.', '_')}"
        param_type = "String"
        is_array = False
        single_flag = condition.get("single_flag", False)
        match_type = condition.get("match_type", None)
        compar_type = condition.get("compar_type", "eq")
        
        for col in columns:
            if col["variable_name"] == field:
                param_type = generate_java_type(col["variable_type"])
                is_array = col["is_array"]
                if is_array and not single_flag:
                    param_type = f"List<{param_type}>"
                break
        if "." in field and "pos" in field:
            param_type = "Double"
        elif field == "zone_id":
            param_type = "Integer"
        elif field == "start_pos" or field == "end_pos":
            param_type = "Vector2"
        
        param_variations.append((param_type, param_name))
        
        if match_type == "ANY":
            filter_conditions.append(f'Filters.in("{field}", {param_name})')
        elif match_type == "ALL":
            filter_conditions.append(f'Filters.all("{field}", {param_name})')
        else:
            filter_map = {
                ">=": "gte",
                "<=": "lte",
                ">": "gt",
                "<": "lt",
                "!=": "ne",
                "eq": "eq"
            }
            filter_conditions.append(
                f'Filters.{filter_map.get(compar_type, "eq")}("{field}", {param_name}' +
                ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            )
    
    filter_str = ', '.join(filter_conditions)
    filter_clause = "new Document()" if not filter_conditions else (
        f"Filters.and({filter_str})" if len(filter_conditions) > 1 else filter_str
    )
    
    # Generate cache filter for direct args
    cache_conditions = []
    for condition in where_conditions:
        field = condition["comparison"]
        param_name = f"where_{field.replace('.', '_')}"
        match_type = condition.get("match_type", None)
        compar_type = condition.get("compar_type", "eq")
        single_flag = condition.get("single_flag", False)
        if match_type == "ANY":
            if single_flag:
                cache_conditions.append(f'{get_getter_name(field, "item")}.contains({param_name})')
            else:
                cache_conditions.append(f'!Collections.disjoint({get_getter_name(field, "item")}, {param_name})')
        elif match_type == "ALL":
            cache_conditions.append(f'{get_getter_name(field, "item")}.containsAll({param_name})')
        else:
            if compar_type == ">=":
                cache_conditions.append(f'{get_getter_name(field, "item")} >= {param_name}')
            elif compar_type == "<=":
                cache_conditions.append(f'{get_getter_name(field, "item")} <= {param_name}')
            elif compar_type == ">":
                cache_conditions.append(f'{get_getter_name(field, "item")} > {param_name}')
            elif compar_type == "<":
                cache_conditions.append(f'{get_getter_name(field, "item")} < {param_name}')
            elif compar_type == "!=":
                cache_conditions.append(f'!Objects.equals({get_getter_name(field, "item")}, {param_name})')
            else:
                cache_conditions.append(f'Objects.equals({get_getter_name(field, "item")}, {param_name})')
    
    cache_filter = f"""
                List<{class_name}> resultList = cache_data.stream()
                    .filter(item -> {{
                        boolean match = true;
                        {''.join(f'match &= {cond};' for cond in cache_conditions)}
                        return match;
                    }})
                    .collect(Collectors.toList());
                {'return DataBaseResultPair.of(false, null);' if not cache_conditions else 'return resultList.isEmpty() ? DataBaseResultPair.of(false, null) : DataBaseResultPair.of(true, resultList.getFirst());' if is_limit_one else 'return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
    """
    
    # Generate set_data filter conditions
    cache_conditions_data = []
    where_args_data = []
    for condition in where_conditions:
        field = condition["comparison"]
        match_type = condition.get("match_type", None)
        compar_type = condition.get("compar_type", "eq")
        single_flag = condition.get("single_flag", False)
        param_name = f"where_{field.replace('.', '_')}"
        java_type = "String"
        is_array = False
        for col in columns:
            if col["variable_name"] == field:
                java_type = generate_java_type(col["variable_type"])
                is_array = col["is_array"]
                if is_array and not single_flag:
                    java_type = f"List<{java_type}>"
                break
        if "." in field and "pos" in field:
            java_type = "Double"
        elif field == "zone_id":
            java_type = "Integer"
        elif field == "start_pos" or field == "end_pos":
            java_type = "Vector2"
        if condition.get("fixed_flag", False):
            where_args_data.append((java_type, param_name))
        if match_type == "ANY":
            if single_flag:
                cache_conditions_data.append(f'{get_getter_name(field, "item")}.contains({(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)})')
            else:
                cache_conditions_data.append(f'!Collections.disjoint({get_getter_name(field, "item")}, {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)})')
        elif match_type == "ALL":
            cache_conditions_data.append(f'{get_getter_name(field, "item")}.containsAll({(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)})')
        else:
            if compar_type == ">=":
                cache_conditions_data.append(f'{get_getter_name(field, "item")} >= {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)}')
            elif compar_type == "<=":
                cache_conditions_data.append(f'{get_getter_name(field, "item")} <= {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)}')
            elif compar_type == ">":
                cache_conditions_data.append(f'{get_getter_name(field, "item")} > {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)}')
            elif compar_type == "<":
                cache_conditions_data.append(f'{get_getter_name(field, "item")} < {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)}')
            elif compar_type == "!=":
                cache_conditions_data.append(f'!Objects.equals({get_getter_name(field, "item")}, {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)})')
            else:
                cache_conditions_data.append(f'Objects.equals({get_getter_name(field, "item")}, {(get_getter_name(field, "set_data") if not condition.get("fixed_flag", False) else param_name)})')
    
    cache_filter_data = f"""
                List<{class_name}> resultList = cache_data.stream()
                    .filter(item -> {{
                        boolean match = true;
                        {''.join(f'match &= {cond};' for cond in cache_conditions_data)}
                        return match;
                    }})
                    .collect(Collectors.toList());
                {'return DataBaseResultPair.of(false, null);' if not cache_conditions_data else 'return resultList.isEmpty() ? DataBaseResultPair.of(false, null) : DataBaseResultPair.of(true, resultList.getFirst());' if is_limit_one else 'return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
"""
    
    # Fixed: Simplify filter_str_data to avoid syntax error
    filter_str_data = []
    for cond in where_conditions:
        field = cond["comparison"]
        compar_type = cond.get("compar_type", "eq")
        if compar_type in ["gte", "lte", "gt", "lt", "ne"]:
            filter_op = compar_type
        else:
            filter_op = "eq"
        param_value = get_getter_name(field, "set_data") if not cond.get("fixed_flag", False) else f"where_{field.replace('.', '_')}"
        filter_str = f'Filters.{filter_op}("{field}", {param_value}' + ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
        filter_str_data.append(filter_str)
    
    filter_str_data = ', '.join(filter_str_data)
    filter_clause_data = "new Document()" if not filter_str_data else (
        f"Filters.and({filter_str_data})" if len(where_conditions) > 1 else filter_str_data
    )
    
    # Method 1: No ClientSession, direct args (SELECT)
    if query_type == "SELECT":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        methods.append(f"""
    public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db{', ' + param_str if param_str else ''}) {{
        try {{
            if (cache_data != null) {{
                {cache_filter}
            }}
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            FindIterable<Document> results = collection.find({filter_clause}){sort_str}{limit_str};
            {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
        }}
    }}""")
    
    # Method 2: With ClientSession, direct args (SELECT)
    if query_type == "SELECT":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        methods.append(f"""
    public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, ClientSession session{', ' + param_str if param_str else ''}) {{
        try {{
            if (cache_data != null) {{
                {cache_filter}
            }}
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            FindIterable<Document> results = collection.find(session, {filter_clause}){sort_str}{limit_str};
            {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
        }}
    }}""")
    
    # Method 3 & 4: set_data methods (SELECT)
    if query_type == "SELECT":
        extra_args = where_args_data
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in extra_args)
        methods.append(f"""
    public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}) {{
        try {{
            if (cache_data != null) {{
                {cache_filter_data}
            }}
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            FindIterable<Document> results = collection.find({filter_clause_data}){sort_str}{limit_str};
            {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
        }}
    }}""")
        
        methods.append(f"""
    public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}) {{
        try {{
            if (cache_data != null) {{
                {cache_filter_data}
            }}
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            FindIterable<Document> results = collection.find(session, {filter_clause_data}){sort_str}{limit_str};
            {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
        }}
    }}""")
    
    # UPDATE methods
    if query_type == "UPDATE":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        set_args = []
        set_clauses = query.get("set", [])
        update_strs = []
        for set_clause in set_clauses:
            field = set_clause["renewal"]
            details_type = set_clause.get("details_type")
            java_type = next((col["variable_type"] for col in columns if col["variable_name"] == field), "Object")
            java_type = generate_java_type(java_type)
            is_array = next((col["is_array"] for col in columns if col["variable_name"] == field), False)
            set_param_name = f"set_{field}"
            if details_type == "Add" and set_clause.get("fixed_flag", False) and not is_array:
                update_strs.append(f"inc(\"{field}\", {set_param_name})")
                set_args.append((java_type, set_param_name))
            elif details_type == "Subtract" and set_clause.get("fixed_flag", False) and not is_array:
                update_strs.append(f"inc(\"{field}\", -{set_param_name})")
                set_args.append((java_type, set_param_name))
            elif details_type == "Add":
                if set_clause.get("fixed_flag", False):
                    update_strs.append(f"addEachToSet(\"{field}\", {set_param_name})")
                    set_args.append((f"List<{java_type}>", set_param_name))
                else:
                    update_strs.append(f"addToSet(\"{field}\", {set_param_name})")
                    set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
            elif details_type == "Delete":
                if set_clause.get("fixed_flag", False):
                    update_strs.append(f"pullAll(\"{field}\", {set_param_name})")
                    set_args.append((f"List<{java_type}>", set_param_name))
                else:
                    update_strs.append(f"pull(\"{field}\", {set_param_name})")
                    set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
            else:
                update_strs.append(f"set(\"{field}\", {set_param_name})")
                set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
        update_str = f"combine({', '.join(update_strs)})" if len(update_strs) > 1 else update_strs[0]
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in set_args)
        
        # UPDATE Method 1: No ClientSession, direct args
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, {param_str}{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            UpdateResult result = collection.updateOne({filter_clause}, Updates.{update_str});
            if (memory_update && result.getModifiedCount() > 0 && cache_data != null && "{unique_field}" != null) {{
                Document updatedDoc = collection.find({filter_clause}).first();
                if (updatedDoc != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), updatedData.{unique_field_getter}()));
                    cache_data.add(updatedData);
                }}
            }}
            return DataBaseResultPair.of(result.getModifiedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        
        # UPDATE Method 2: With ClientSession, direct args
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {param_str}{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            UpdateResult result = collection.updateOne(session, {filter_clause}, Updates.{update_str});
            if (memory_update && result.getModifiedCount() > 0 && cache_data != null && "{unique_field}" != null) {{
                Document updatedDoc = collection.find(session, {filter_clause}).first();
                if (updatedDoc != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), updatedData.{unique_field_getter}()));
                    cache_data.add(updatedData);
                }}
            }}
            return DataBaseResultPair.of(result.getModifiedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        
        # UPDATE Method 3 & 4: set_data methods
        set_filter_str = []
        for cond in where_conditions:
            field = cond["comparison"]
            compar_type = cond.get("compar_type", "eq")
            if compar_type in ["gte", "lte", "gt", "lt", "ne"]:
                filter_op = compar_type
            else:
                filter_op = "eq"
            param_value = get_getter_name(field, "set_data") if not cond.get("fixed_flag", False) else f"where_{field.replace('.', '_')}"
            filter_str = f'Filters.{filter_op}("{field}", {param_value}' + ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            set_filter_str.append(filter_str)
        
        set_filter_str = ', '.join(set_filter_str)
        set_filter_clause = "new Document()" if not set_filter_str else (
            f"Filters.and({set_filter_str})" if len(where_conditions) > 1 else set_filter_str
        )
        set_update_strs = []
        set_args_data = []
        for set_clause in set_clauses:
            field = set_clause["renewal"]
            details_type = set_clause.get("details_type")
            java_type = next((col["variable_type"] for col in columns if col["variable_name"] == field), "Object")
            java_type = generate_java_type(java_type)
            is_array = next((col["is_array"] for col in columns if col["variable_name"] == field), False)
            set_param_name = f"set_{field}"
            getter_name = get_getter_name(field, "set_data")
            if details_type == "Add" and set_clause.get("fixed_flag", False) and not is_array:
                set_update_strs.append(f"inc(\"{field}\", {set_param_name})")
                set_args_data.append((java_type, set_param_name))
            elif details_type == "Subtract" and set_clause.get("fixed_flag", False) and not is_array:
                set_update_strs.append(f"inc(\"{field}\", -{set_param_name})")
                set_args_data.append((java_type, set_param_name))
            elif details_type == "Add":
                if set_clause.get("fixed_flag", False):
                    set_update_strs.append(f"addEachToSet(\"{field}\", {set_param_name})")
                    set_args_data.append((f"List<{java_type}>", set_param_name))
                else:
                    set_update_strs.append(f"addToSet(\"{field}\", {getter_name})")
            elif details_type == "Delete":
                if set_clause.get("fixed_flag", False):
                    set_update_strs.append(f"pullAll(\"{field}\", {set_param_name})")
                    set_args_data.append((f"List<{java_type}>", set_param_name))
                else:
                    set_update_strs.append(f"pull(\"{field}\", {getter_name})")
            else:
                set_update_strs.append(f"set(\"{field}\", {(set_param_name if set_clause.get('fixed_flag', False) else getter_name)})")
                if set_clause.get("fixed_flag", False):
                    set_args_data.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
        set_update_str = f"combine({', '.join(set_update_strs)})" if len(set_update_strs) > 1 else set_update_strs[0]
        extra_args_str_data = ', '.join(f"{arg[0]} {arg[1]}" for arg in set_args_data)
        
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str_data if extra_args_str_data else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            UpdateResult result = collection.updateOne({set_filter_clause}, Updates.{set_update_str});
            if (memory_update && result.getModifiedCount() > 0 && cache_data != null && "{unique_field}" != null) {{
                Document updatedDoc = collection.find({set_filter_clause}).first();
                if (updatedDoc != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), updatedData.{unique_field_getter}()));
                    cache_data.add(updatedData);
                }}
            }}
            return DataBaseResultPair.of(result.getModifiedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str_data if extra_args_str_data else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            UpdateResult result = collection.updateOne(session, {set_filter_clause}, Updates.{set_update_str});
            if (memory_update && result.getModifiedCount() > 0 && cache_data != null && "{unique_field}" != null) {{
                Document updatedDoc = collection.find(session, {set_filter_clause}).first();
                if (updatedDoc != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), updatedData.{unique_field_getter}()));
                    cache_data.add(updatedData);
                }}
            }}
            return DataBaseResultPair.of(result.getModifiedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
    
    # DELETE methods
    if query_type == "DELETE":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, {param_str}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            Document doc = collection.find({filter_clause}).first();
            DeleteResult result = collection.deleteOne({filter_clause});
            if (memory_update && result.getDeletedCount() > 0 && cache_data != null && doc != null && "{unique_field}" != null) {{
                {class_name} deletedData = new {class_name}(doc);
                cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), deletedData.{unique_field_getter}()));
            }}
            return DataBaseResultPair.of(result.getDeletedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {param_str}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            Document doc = collection.find(session, {filter_clause}).first();
            DeleteResult result = collection.deleteOne(session, {filter_clause});
            if (memory_update && result.getDeletedCount() > 0 && cache_data != null && doc != null && "{unique_field}" != null) {{
                {class_name} deletedData = new {class_name}(doc);
                cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), deletedData.{unique_field_getter}()));
            }}
            return DataBaseResultPair.of(result.getDeletedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        
        # DELETE Method 3 & 4: set_data methods
        extra_args = where_args_data
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in extra_args)
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            Document doc = collection.find({filter_clause_data}).first();
            DeleteResult result = collection.deleteOne({filter_clause_data});
            if (memory_update && result.getDeletedCount() > 0 && cache_data != null && doc != null && "{unique_field}" != null) {{
                {class_name} deletedData = new {class_name}(doc);
                cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), deletedData.{unique_field_getter}()));
            }}
            return DataBaseResultPair.of(result.getDeletedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            Document doc = collection.find(session, {filter_clause_data}).first();
            DeleteResult result = collection.deleteOne(session, {filter_clause_data});
            if (memory_update && result.getDeletedCount() > 0 && cache_data != null && doc != null && "{unique_field}" != null) {{
                {class_name} deletedData = new {class_name}(doc);
                cache_data.removeIf(item -> Objects.equals(item.{unique_field_getter}(), deletedData.{unique_field_getter}()));
            }}
            return DataBaseResultPair.of(result.getDeletedCount() > 0, null);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
    
    # INSERT methods
    if query_type == "INSERT":
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, {class_name} data, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            collection.insertOne(data.toDocument());
            if (memory_update && cache_data != null) {{
                cache_data.add(data);
            }}
            return DataBaseResultPair.of(true, data);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
        methods.append(f"""
    public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} data, boolean memory_update) {{
        try {{
            MongoCollection<Document> collection = db.getCollection("{collection_name}");
            collection.insertOne(session, data.toDocument());
            if (memory_update && cache_data != null) {{
                cache_data.add(data);
            }}
            return DataBaseResultPair.of(true, data);
        }} catch (Exception e) {{
            return DataBaseResultPair.of(false, null);
        }}
    }}""")
    
    return "\n".join(methods)
def generate_db_class(class_name, queries, columns, collection_name):
    query_methods = "\n".join(generate_query_methods(query, collection_name, class_name, columns) for query in queries) if queries else ""
    
    return f"""import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class {class_name.replace("Data","")}Db {{
    public static final String collection_name = "{collection_name}";
    public static List<{class_name}> cache_data;

    public static void MemoryCache{class_name}(MongoDatabase db) {{
        MongoCollection<Document> collection = db.getCollection(collection_name);
        FindIterable<Document> results = collection.find();
        cache_data = new ArrayList<>();
        for (Document doc : results) {{
            cache_data.add(new {class_name}(doc));
        }}
    }}

    public static void createIndexes(MongoDatabase db) {{
        MongoCollection<Document> collection = db.getCollection(collection_name);
        {''.join(f'collection.createIndex(Indexes.ascending("{col["variable_name"]}"), new IndexOptions().unique({"true" if col["index_type"] == "unique" else "false"}));' for col in columns if col["index_type"] != "none")}
    }}

    public static boolean bulkInsert{class_name}(MongoDatabase db, List<{class_name}> dataList) {{
        try {{
            MongoCollection<Document> collection = db.getCollection(collection_name);
            List<Document> documents = new ArrayList<>();
            for ({class_name} data : dataList) {{
                documents.add(data.toDocument());
            }}
            collection.insertMany(documents);
            return true;
        }} catch (com.mongodb.MongoWriteException e) {{
            if (e.getCode() == 11000) {{
                return false;
            }}
            throw e;
        }} catch (Exception e) {{
            return false;
        }}
    }}

{query_methods}
}}
"""

def generate_java_class(class_name, columns):
    fields = "\n".join(generate_field_declaration(col) for col in columns)
    getters = "\n".join(generate_getter(col) for col in columns)
    setters = "\n".join(generate_setter(col) for col in columns)
    
    doc_constructor_lines = []
    for col in columns:
        var_name = col["variable_name"]
        var_type = col["variable_type"]
        if col["is_array"]:
            doc_constructor_lines.append(f"        this.{var_name} = (List<{generate_java_type(var_type)}>) doc.get(\"{var_name}\");")
        elif var_type == "int":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getInteger(\"{var_name}\");")
        elif var_type == "String":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getString(\"{var_name}\");")
        elif var_type == "double":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getDouble(\"{var_name}\");")
        else:
            doc_constructor_lines.append(f"        Document {var_name}Doc = (Document) doc.get(\"{var_name}\");")
            doc_constructor_lines.append(f"        this.{var_name} = {var_name}Doc != null ? new {generate_java_type(var_type)}({var_name}Doc) : null;")
    doc_constructor = "\n".join(doc_constructor_lines)

    to_doc_lines = ["        Document doc = new Document();"]
    for col in columns:
        var_name = col["variable_name"]
        var_type = col["variable_type"]
        if col["is_array"] or var_type in ["int", "String", "double"]:
            to_doc_lines.append(f"        doc.append(\"{var_name}\", this.{var_name});")
        else:
            to_doc_lines.append(f"        doc.append(\"{var_name}\", this.{var_name} != null ? this.{var_name}.toDocument() : null);")
    to_doc_lines.append("        return doc;")
    to_doc = "\n".join(to_doc_lines)

    return f"""import org.bson.Document;
import java.util.List;

public class {class_name} {{
{fields}

    public {class_name}() {{
    }}

    public {class_name}(Document doc) {{
{doc_constructor}
    }}

    public Document toDocument() {{
{to_doc}
    }}

{getters}
{setters}
}}
"""
def generate_java_code(json_data, collection_name):
    job_type = json_data[collection_name]
    main_class_name = generate_class_name(collection_name)
    main_columns = job_type["column_list"]
    queries = job_type.get("queries", [])
    
    with open(f"{main_class_name}.java", "w") as f:
        f.write(generate_java_class(main_class_name, main_columns))
    print(f"Generated {main_class_name}.java")
    
    with open(f"{main_class_name.replace('Data','')}Db.java", "w") as f:
        f.write(generate_db_class(main_class_name, queries, main_columns, collection_name))
    print(f"Generated {main_class_name}Db.java")
    
    if "customVariables" in job_type:
        for custom_var in job_type["customVariables"]:
            for var_type, columns in custom_var.items():
                custom_class_name = generate_class_name(var_type)
                with open(f"{custom_class_name}.java", "w") as f:
                    f.write(generate_java_class(custom_class_name, columns))
                print(f"Generated {custom_class_name}.java")

def main():
    #json_data = {
    #    "job_type": {
    #        "column_list": [
    #            {"variable_type": "int", "variable_name": "zone_id", "variable_explanation": "ゾーンID", "index_type": "unique", "is_array": False},
    #            {"variable_type": "String", "variable_name": "zone_name", "variable_explanation": "ゾーン名", "index_type": "none", "is_array": False},
    #            {"variable_type": "Vector2", "variable_name": "start_pos", "variable_explanation": "始点", "index_type": "none", "is_array": False},
    #            {"variable_type": "Vector2", "variable_name": "end_pos", "variable_explanation": "終点", "index_type": "none", "is_array": False},
    #            {"variable_type": "int", "variable_name": "chunk_list", "variable_explanation": "チャンクリスト", "index_type": "none", "is_array": True},
    #        ],
    #        "queries": [
    #            #{"type": "SELECT", "where": [{"comparison": "zone_name"}], "order": {"sort": [{"type": "asc", "comparison": "zone_id"}], "limit": 1}, "method_name": "ZoneName"},
    #            #{"type": "SELECT", "where": [{"comparison": "start_pos"}], "method_name": "StartPos"},
    #            {"type": "SELECT", "where": [{"comparison": "start_pos.x", "compar_type": ">="}], "method_name": "StartPosX"},
    #            {"type": "SELECT", "where": [{"comparison": "start_pos"}, {"conditions": "and", "comparison": "end_pos"}], "method_name": "StartEndPos"},
    #            {"type": "SELECT", "where": [{"comparison": "chunk_list", "single_flag": True, "match_type": "ANY"}], "method_name": "ChunkListAny"},
    #            {"type": "UPDATE", "set": [{"renewal": "chunk_list", "details_type": "Add"}], "where": [{"comparison": "zone_id"}], "method_name": "AddChunkList"},
    #            {"type": "UPDATE", "set": [{"renewal": "chunk_list", "details_type": "Add", "fixed_flag": True}], "where": [{"comparison": "zone_id"}], "method_name": "AddChunkListFixed"},
    #            {"type": "UPDATE", "set": [{"renewal": "chunk_list", "details_type": "Delete"}], "where": [{"comparison": "zone_id"}], "method_name": "DeleteChunkList"},
    #            {"type": "UPDATE", "set": [{"renewal": "zone_id", "details_type": "Add","fixed_flag" : True}], "where": [{"comparison": "zone_id"}], "method_name": "AddZoneId"},
    #            {"type": "UPDATE", "set": [{"renewal": "zone_id", "details_type": "Subtract", "fixed_flag": True}], "where": [{"comparison": "zone_id"}], "method_name": "SubtractZoneId"},
    #            {"type": "UPDATE", "set": [{"renewal": "zone_name"}], "where": [{"comparison": "zone_id"}], "method_name": "UpdateZoneName"},
    #            {"type": "UPDATE", "set": [{"renewal": "start_pos"}, {"renewal": "end_pos"}], "where": [{"comparison": "zone_id"}], "method_name": "StartEndPos"},
    #            {"type": "DELETE", "where": [{"comparison": "zone_id"}], "method_name": "findZoneID"},
    #        ],
    #        "customVariables": [
    #            {"Vector2": [
    #                {"variable_type": "double", "variable_name": "x", "variable_explanation": "x", "index_type": "none", "is_array": False},
    #                {"variable_type": "double", "variable_name": "y", "variable_explanation": "y", "index_type": "none", "is_array": False},
    #            ]}
    #        ]
    #    }
    #}
    
    json_data = {
        "item_block_game": {
            "column_list": [
                {"variable_type": "String", "variable_name": "id_name", "variable_explanation": "ID名", "index_type": "unique", "is_array": False},
                {"variable_type": "String", "variable_name": "name_lang", "variable_explanation": "日本語名", "index_type": "none", "is_array": False},
                {"variable_type": "double", "variable_name": "rate", "variable_explanation": "レート", "index_type": "none", "is_array": False},
                {"variable_type": "double", "variable_name": "price", "variable_explanation": "価格", "index_type": "none", "is_array": False},
                {"variable_type": "int", "variable_name": "recipe_create_job_list", "variable_explanation": "作れる職業jobリスト", "index_type": "none", "is_array": True},
                {"variable_type": "int", "variable_name": "action_price_job", "variable_explanation": "アクションしたとき得られるジョブ", "index_type": "none", "is_array": True},
            ],
            "queries": [
                {"type": "SELECT", "method_name": "findAllItemBlock"},
                 {"type": "SELECT", "where": [{"comparison": "id_name"}],  "order": {"limit": 1},"method_name": "findIdName"},
                {"type": "UPDATE", "set": [{"renewal": "recipe_create_job_list", "details_type": "Add"}], "where": [{"comparison": "id_name"}], "method_name": "AddRecipeJob"},
                {"type": "SELECT", "where": [{"comparison": "recipe_create_job_list", "match_type": "ANY", "fixed_flag": True, "single_flag": True}], "method_name": "findJobRecipe"},
                {"type": "SELECT", "where": [{"comparison": "action_price_job", "match_type": "ANY", "fixed_flag": True, "single_flag": True}],                "order": {
                    "limit": 1
                }, "method_name": "findActionJob"},
            ]
        }
    }
    
    a_json_data = {
    "user_game_player": {
        "column_list": [
            {
                "variable_type": "String",
                "variable_name": "player_id",
                "variable_explanation": "プレイヤーID",
                "index_type": "unique",
                "is_array": False
            },
            {
                "variable_type": "String",
                "variable_name": "player_name",
                "variable_explanation": "プレイヤー名",
                "index_type": "none",
                "is_array": False
            },
            {
                "variable_type": "double",
                "variable_name": "balance",
                "variable_explanation": "残高",
                "index_type": "none",
                "is_array": False
            },
            {
                "variable_type": "int",
                "variable_name": "job_id",
                "variable_explanation": "職業ID",
                "index_type": "none",
                "is_array": False
            }
        ],
        "queries": [
            {
                "type": "SELECT",
                "where": [
                    {
                        "comparison": "player_id"
                    }
                ],
                "order": {
                    "limit": 1
                },
                "method_name": "findPlayerGameData"
            },
            {
                "type": "UPDATE",
                "set": [
                    {
                        "renewal": "balance",
                        "details_type": "Add",
                        "fixed_flag" : True
                    }
                ],
                "where": [
                    {
                        "comparison": "player_id"
                    }
                ],
                "method_name": "addBalance"
            },
            {
                "type": "UPDATE",
                "set": [
                    {
                        "renewal": "job_id",
                        
                    }
                ],
                "where": [
                    {
                        "comparison": "player_id"
                    }
                ],
                "method_name": "changeJobID"
            }
        ]
    }
}
    
    b_json_data =     {
      "job_type": {
        "column_list": [
          {
            "variable_type": "int",
            "variable_name": "job_id",
            "variable_explanation": "職業ID",
            "index_type": "unique",
            "is_array": False
          },
          {
            "variable_type": "double",
            "variable_name": "sell_ratio",
            "variable_explanation": "売買倍率",
            "index_type": "none",
            "is_array": False
          },
          {
            "variable_type": "String",
            "variable_name": "job_name",
            "variable_explanation": "職業名",
            "index_type": "none",
            "is_array": False
          }
        ],
        "queries": [
          {
            "type": "SELECT",
            "method_name": "findAllJobType"
          },
                      {
                    "type": "SELECT",
                    "where": [
                        {
                            "comparison": "job_id"
                        }
                    ],
                    "order": {
                        "limit": 1
                    },
                    "method_name": "findJobId"
                },
        ]
      }
    }

    
    generate_java_code(json_data, "item_block_game")

if __name__ == "__main__":
    main()