#light

open Ast
open Util
open Types
open Extensions



type TypeContext = Map<string, Type>
and RemainingMap = Map<string, TypeContext -> TypeContext>

let matchingEntity fields types =
  Map.tryPick (fun k v -> match v with
                          | TyType (fields', _) when fields = fields' -> Some k
                          | _ -> None)
              types

let rec types (env:TypeContext) = function
  | Def (Identifier name, expr) ->
      let typ = typeOf env expr
      env.Add(name, typ)
  | Expr expr -> typeOf env expr |> ignore
                 env
  | Function (Identifier name, parameters, retType, body) ->
      let env', fnType =
        List.foldBack (fun (Param (Identifier param, typ)) (env:TypeContext, fnType)  ->
                         match typ with
                         | Some t -> env.Add(param, t), TyArrow (t, fnType)
                         | _ -> failwithf "You must annotate all function arguments with their type!")
                      parameters (env, retType)
      // Add itself
      let env'' = env'.Add(name, fnType)
      if typeOf env'' body = retType
        then env.Add(name, fnType)
        else failwithf "The function body doesn't return %A" retType
  | Entity (Identifier name, ((source, Symbol uniqueId), assocs, members)) ->
      match typeOf env source with
      | TyStream (TyRecord fields) ->
          let members1 =
            List.fold (fun (acc:TypeContext) assoc -> 
                         match assoc with
                         | BelongsTo (Symbol entity) ->
                             let entityName = String.capitalize entity
                             acc.Add(entity, TyRef (TyEntity entityName))
                         | HasMany (Symbol entity) ->
                             let entityName = String.capitalize entity |> String.singular
                             acc.Add(entity, TyDict (TyRef (TyEntity entityName))))
                      (fields) assocs
                                  
          let members2 = List.fold (fun (acc:TypeContext) (Member (Identifier self, Identifier name, expr)) ->
                                      let selfType = TyRecord acc
                                      Map.add name (typeOf (env.Add(self, selfType)) expr) acc)
                                   members1 members
                                   
          let entityType = TyType (members2, uniqueId)
          env.Add(name, entityType).Add(sprintf "$%s_all" name, TyDict (TyEntity name))
      | _ -> failwithf "The source of the entity '%s' is not a stream" name

and typeOf env expr =
  match expr with
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
  | FuncCall (Id (Identifier "when"), [source; Lambda ([Param (Identifier ev, _)], handler)]) ->
      match typeOf env source with
      | TyStream _ -> ()
      | _ -> failwithf "The source of the when doesn't reduce to a stream"
      let evType = TyRecord (Map.of_list ["timestamp", TyInt; "value", TyInt])
      typeOf (env.Add(ev, evType)) handler |> ignore
      TyUnit
  | FuncCall (Id (Identifier "print"), [expr]) ->
      typeOf env expr |> ignore
      TyUnit
  | FuncCall (Id (Identifier "$ref"), [expr]) -> TyRef (typeOf env expr)
  | FuncCall (f, param's) ->
      let rec getReturnType argCount funType =
        match argCount, funType with
        | 0, _ -> funType
        | n, TyArrow (type1, type2) when n > 0 -> getReturnType (n - 1) type2
        | _ -> failwithf "%A must be of type TyArrow!" funType
      
      let rec arrow2list = function
        | TyArrow (type1, type2) -> type1::(arrow2list type2)
        | other -> [other]
        
      let funType = typeOf env f
      let argTypes = arrow2list funType |> Seq.take param's.Length |> Seq.to_list      
      let matching = List.forall2 (fun arg paramType -> (typeOf env arg) = paramType) param's argTypes
      if matching
        then getReturnType param's.Length funType
        else failwithf "Parameters don't match argument types in function call"
  | MemberAccess (target, Identifier name) ->
      let targetType = match typeOf env target with
                       | TyRef t -> t
                       | other -> other
                       
      match targetType with
      | TyRecord fields ->
          match Map.tryFind name fields with
          | Some t -> t
          | None -> failwithf "The record doesn't have field '%s'" name
      | TyEntity t ->
          let fields = match env.[t] with
                       | TyType (fields, _) -> fields
                       | _ -> failwithf "The entity is not a TyType?!"
          match Map.tryFind name fields with
          | Some t -> t
          | None -> failwithf "The entity doesn't have field '%s'" name
      | TyType (fields, _) -> match name with
                              | "all" -> TyDict (TyRecord fields)
                              | _ -> failwithf "The type %A does not have field '%s'" targetType name
      | _ -> failwithf "The target type %A doesn't have any fields." targetType
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | Record fields ->
      let fields = Map.of_list [ for (name, expr) in fields -> (name, typeOf env expr) ]
      match matchingEntity fields env with
      | Some entity -> TyEntity entity
      | _ -> TyRecord fields
  | RecordWith (source, newFields) ->
      match typeOf env source with
      | TyRecord fields -> TyRecord (List.fold (fun fields (name, expr) -> fields.Add(name, typeOf env expr)) fields newFields)
      | _ -> failwith "Source is not a record!"
  | Let (Identifier name, optType, binder, body) ->
      match optType with
      | None -> typeOf (env.Add(name, typeOf env binder)) body
      | Some typ -> typeOf (env.Add(name, typ)) body
  | Lambda (args, expr) ->
      let env', argTypes =
        List.fold (fun (env:TypeContext, argTypes) (Param (Identifier id, typ)) ->
                     match typ with
                     | Some typ' -> env.Add(id, typ'), argTypes @ [typ']
                     | None -> failwithf "I have no idea what's the type of argument %A" id)
                  (env, []) args
      let funType = List.foldBack (fun arg acc -> TyArrow (arg, acc)) argTypes (typeOf env' expr)
      funType
      //TyLambda (argTypes, typeOf env' expr)
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
      match Map.tryFind name env with
      | Some v -> v
      | _ -> raise (UnknownId name)
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
          | [SymbolExpr (Symbol field)] when Map.contains field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Param (Identifier ev, _)], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          match paramExps with
          | [Lambda ([Param (Identifier ev, _)], expr)] -> 
              match typeOf (env.Add(ev, evType)) expr with
              | TyRecord projFields -> TyStream (TyRecord (projFields.Add("timestamp", TyInt)))
              | _ -> failwithf "The projector of the select doesn't return a record!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps        
      | "[]" ->
          match paramExps with
          | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Param (Identifier g, _)], expr)] when Map.contains field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  // Event Windows
  | TyWindow (TyStream (TyRecord fields as evType), TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when Map.contains field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Param (Identifier ev, _)], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Param (Identifier g, _)], expr)] when Map.contains field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
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
          | [Lambda ([Param (Identifier g, _)], expr)] ->
              if (typeOf (env.Add(g, valueType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          let valueType' = match paramExps with
                           | [Lambda ([Param (Identifier g, _)], expr)] -> typeOf (env.Add(g, valueType)) expr
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
  | TyRecord _ -> typeOf env (FuncCall (MemberAccess (target, Identifier name), paramExps))
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
