#light

open Ast
open Types
open Extensions

type WindowType =
  | TimedWindow of int

type Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyString
  | TySymbol
  | TyType of Map<string, Type>
  | TyRecord of Map<string, Type>
  | TyStream of Type
  | TyWindow of Type * WindowType
  | TyDict of Type
  
  override self.ToString() =
    match self with
    | TyUnit -> "()"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyType _ -> "type"
    | TyRecord _ -> "record"
    | TyStream _ -> "stream"
    | TyWindow _ -> "window"
    | TyDict _ -> "dict"


and TypeContext = Map<string, Type>
and RemainingMap = Map<string, TypeContext -> TypeContext>

let rec types ((env:TypeContext), remaining:RemainingMap) = function
  | Def (Identifier name, expr) ->
      let typ = typeOf env expr
      env.Add(name, typ), remaining
  | Expr expr -> typeOf env expr |> ignore
                 env, remaining
  | Entity (Identifier name, ((source, uniqueId), assocs, members)) ->
      match typeOf env source with
      | TyStream (TyRecord fields) ->
          let members1, remaining' =
            List.fold_left (fun (acc:TypeContext, rem:RemainingMap) assoc -> 
                              match assoc with
                              | BelongsTo (Symbol entity) ->
                                  let entityName = String.capitalize entity
                                  let fieldType = match env.[entityName] with
                                                  | TyType fields -> TyRecord fields
                                                  | _ -> failwithf "The entity is not an entity :S"
                                  let acc' = acc.Add(entity, fieldType)
                                  acc', rem
                              | HasMany (Symbol entity) ->
                                  let entityName = String.capitalize entity |> String.singular
                                  // If the related entity has already been typechecked, add its type to
                                  // the accumulator. Otherwise, add a continuation specifying what to do when
                                  // the entity is typechecked.
                                  let rec findType env =
                                    match Map.tryfind entityName env with
                                    | Some (TyType fields as e) -> acc.Add(entity, e), rem
                                    | _ -> acc, rem.Add(entityName, fun env -> env.Add(name, TyType (fst (findType env))))
                                  findType env)
                           (fields, remaining) assocs
                                        
          let members2 = List.fold_left (fun (acc:TypeContext) (Member (Identifier self, Identifier name, expr)) ->
                                           let selfType = TyRecord acc
                                           Map.add name (typeOf (env.Add(self, selfType)) expr) acc)
                                        members1 members
          let env' = env.Add(name, TyType members2).Add(sprintf "$%s_all" name, TyDict (TyRecord members2))

          // If there is any entity waiting on this one to be typechecked,
          // now is a good time to do it.
          match Map.tryfind name remaining' with
          | Some k -> k env', Map.remove name remaining'
          | _ -> env', remaining'
      | _ -> failwithf "The source of the entity '%s' is not a stream" name

and typeOf env = function
  | MethodCall (target, (Identifier name), paramExps) ->
      typeOfMethodCall env target name paramExps
  | ArrayIndex (target, index) ->
      typeOfMethodCall env target "[]" [index]
  | FuncCall (Id (Identifier "stream"), fields) ->
      let fields' = [ for f in fields ->
                        match f with
                        | SymbolExpr (Symbol name) -> (name, TyInt)
                        | _ -> failwithf "Invalid arguments to 'stream': %A" f ]
      TyStream (TyRecord (Map.of_list (("timestamp", TyInt)::fields')))
  | FuncCall (Id (Identifier "when"), [source; Lambda ([Identifier ev], handler)]) ->
      match typeOf env source with
      | TyStream _ -> ()
      | _ -> failwithf "The source of the when doesn't reduce to a stream"
      let evType = TyRecord (Map.of_list ["timestamp", TyInt; "value", TyInt])
      typeOf (env.Add(ev, evType)) handler |> ignore
      TyUnit
  | FuncCall (Id (Identifier "print"), [expr]) ->
      typeOf env expr |> ignore
      TyUnit
  | MemberAccess (target, Identifier name) ->
      let targetType = typeOf env target
      match targetType with
      | TyRecord fields -> match Map.tryfind name fields with
                           | Some t -> t
                           | None -> failwithf "The record doesn't have field '%s'" name
      | TyType fields -> match name with
                         | "all" -> TyDict (TyRecord fields)
                         | _ -> failwithf "The type %A does not have field '%s'" targetType name
      | _ -> failwithf "The target type %A doesn't have any fields." targetType
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | Record fields ->
      let fieldTypes = [ for (Symbol name, expr) in fields -> (name, typeOf env expr) ]
      TyRecord (Map.of_list fieldTypes)
  | RecordWith (source, newFields) ->
      match typeOf env source with
      | TyRecord fields -> TyRecord (List.fold_left (fun fields (Symbol name, expr) -> fields.Add(name, typeOf env expr)) fields newFields)
      | _ -> failwith "Source is not a record!"
  | Let (Identifier name, binder, body) -> typeOf (env.Add(name, typeOf env binder)) body
  | If (cond, thn, els) ->
      let tyCond = typeOf env cond
      let tyThn = typeOf env thn
      let tyEls = typeOf env els
      if tyCond <> TyBool then failwith "If: The condition doesn't return bool!"
      if tyThn <> tyEls then failwith "If: Then and else have different return types."
      tyThn
  | Seq (expr1, expr2) ->
      typeOf env expr1 |> ignore
      typeOf env expr2
  | Id (Identifier name) ->
      match Map.tryfind name env with
      | Some v -> v
      | _ -> failwithf "typeOf: Unknown variable or identifier: %s" name  
  | SymbolExpr _ -> TySymbol
  | Integer v -> TyInt
  | String s -> TyString
  | Bool b -> TyBool
  | other -> failwithf "typeOf: Not implemented: %A" other


and typeOfMethodCall env target name paramExps =
  let targetType = typeOf env target
  match targetType with
  | TyStream (TyRecord fields as evType) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when Map.mem field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              match typeOf (env.Add(ev, evType)) expr with
              | TyRecord projFields as outEvType -> TyStream outEvType
              | _ -> failwithf "The projector of the select doesn't return a record!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps        
      | "[]" ->
          match paramExps with
          | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when Map.mem field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  // Event Windows
  | TyWindow (TyStream (TyRecord fields as evType), TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when Map.mem field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when Map.mem field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyWindow (TyInt, TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [] -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyDict valueType ->
      match name with
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier g], expr)] -> if (typeOf (env.Add(g, valueType)) expr) <> TyBool
                                                 then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          let valueType' = match paramExps with
                           | [Lambda ([Identifier g], expr)] -> typeOf (env.Add(g, valueType)) expr
                           | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          TyDict valueType'
      | "[]" -> match paramExps with
                | [index] -> valueType
                | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyInt ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [] -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "[]" -> match paramExps with
                | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
                | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "updated" -> match paramExps with
                     | [] -> TyStream (TyRecord (Map.of_list ["value", TyInt]))
                     | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | _ -> failwithf "The type %A does not have method %A!" targetType name
 

and typeOfOp = function
  | Plus, TyInt, TyInt -> TyInt
  | Minus, TyInt, TyInt -> TyInt
  | Times, TyInt, TyInt -> TyInt
  | GreaterThan, TyInt, TyInt -> TyBool
  | GreaterThanOrEqual, TyInt, TyInt -> TyBool
  | Equal, TyInt, TyInt -> TyBool
  | NotEqual, TyInt, TyInt -> TyBool
  | LessThanOrEqual, TyInt, TyInt -> TyBool
  | LessThan, TyInt, TyInt -> TyBool
  | Plus, _, TyString -> TyString
  | Plus, TyString, _ -> TyString
  | x -> failwithf "typeOfOp: op not implemented %A" x
