open Ast
open Types
open Extensions
open Util

// Shamelessly stolen and adapted from Andrej Bauer's "Programming Language Zoo"
// -- http://andrej.com/plzoo/html/poly.html
let rec occurs k = function
  | TyInt | TyFloat | TyBool | TyString | TyUnit | TySymbol | TyAlias _ | TyPrimitive _ -> false
  | TyVariant (_, variants) -> List.exists (occurs k) (List.map snd variants)
  | TyStream ty | TyWindow ty | TyUnknown ty | TyDict ty -> occurs k ty
  | TyGen j -> k = j
  | TyArrow (t1, t2) -> occurs k t1 || occurs k t2
  | TyRecord fields -> Map.exists (fun _ v -> occurs k v) fields
  | TyTuple elts -> List.exists (occurs k) elts


// Apply the substitutions in the subst list.
let rec typeSubst subst t =
  match t with
    | TyInt | TyFloat | TyBool | TyString | TyUnit | TySymbol | TyPrimitive _ | TyAlias _ -> t
    | TyStream ty -> TyStream (typeSubst subst ty)
    | TyWindow ty -> TyWindow (typeSubst subst ty)
    | TyUnknown ty -> TyUnknown (typeSubst subst ty)
    | TyDict ty -> TyDict (typeSubst subst ty)
    | TyArrow (t1, t2) -> TyArrow (typeSubst subst t1, typeSubst subst t2)
    | TyRecord fields -> TyRecord (Map.map (fun _ v -> typeSubst subst v) fields)
    | TyTuple elts -> TyTuple (List.map (typeSubst subst) elts)
    | TyVariant (name, variants) -> TyVariant (name, List.map (fun (label, ty) -> label, typeSubst subst ty) variants)
    | TyGen k -> match List.tryFind (fun (k', _) -> k' = k) subst with
                 | Some (s, t) -> t
                 | _ -> TyGen k

// Replace alias with their real types
// TODO: Merge with typeSubst
let rec replaceAliases (env:TypeContext) = function
  | (TyInt | TyFloat | TyBool | TyString | TyUnit | TySymbol | TyPrimitive _ | TyGen _) as t -> t
  | TyStream ty -> TyStream (replaceAliases env ty)
  | TyWindow ty -> TyWindow (replaceAliases env ty)
  | TyUnknown ty -> TyUnknown (replaceAliases env ty)
  | TyDict ty -> TyDict (replaceAliases env ty)
  | TyAlias alias -> fst env.[alias]
  | TyArrow (t1, t2) -> TyArrow (replaceAliases env t1, replaceAliases env t2)
  | TyRecord fields -> TyRecord (Map.map (fun _ v -> replaceAliases env v) fields)
  | TyTuple elts -> TyTuple (List.map (replaceAliases env) elts)
  | TyVariant (name, variants) -> TyVariant (name, List.map (fun (label, ty) -> label, replaceAliases env ty) variants)

// Produces fresh new generic variables with incremental identifiers and a
// counter that returns the next identifier
let fresh, counter =
  let counter = ref 0
  (fun () ->
    let label = !counter
    counter := !counter + 1
    TyGen label),
  (fun () -> !counter)


let solve constr =
  let rec unify constr subst =
    match constr with
    | [] -> subst
    | c::cs ->
        //printfn "%A\n %A\n\n\n\n\n\n" constr subst
        match c with
        | (s, t) when s = t -> unify cs subst
        | (TyGen s, t) | (t, TyGen s) when not (occurs s t) ->
            let sub = typeSubst [s, t]
            unify (List.map (fun (a, b) -> (sub a, sub b)) cs)
                  ((s, t)::(List.map (fun (k, v) -> (k, sub v)) subst))
        | (TyArrow (a1, r1), TyArrow (a2, r2)) -> unify ((a1, a2)::(r1, r2)::cs) subst
        | (TyTuple tys1, TyTuple tys2) -> unify ((List.zip tys1 tys2) @ cs) subst
        | (TyRecord fields1, TyRecord fields2) ->
            let labels1 = Map.keySet fields1
            let labels2 = Map.keySet fields2
            if labels1 = labels2
              then let toCheck = [ for l in labels1 -> (fields1.[l], fields2.[l]) ]
                   unify (toCheck @ cs) subst
              else failwithf "Can't unify %A" c
        | (TyStream t1, TyStream t2) -> unify ((t1, t2)::cs) subst
        | (TyDict t1, TyDict t2) -> unify ((t1, t2)::cs) subst
        | _ -> failwithf "Can't unify %A" c


  unify constr []

(* Generalize (i.e., change the ids to newly generated ones) any generic
 * variables whose threshold is greater or equal than the given one.
 * This operation must be consistent, i.e., if the same generic variable 'x is
 * used in more than one place, all the uses must be replaced with the SAME new
 * fresh variable.
 *)
let rec generalize ty threshold (subst:(int * Type) list) =
  match ty with
  | TyInt | TyFloat | TyBool | TyString | TyUnit | TySymbol | TyPrimitive _ | TyAlias _ -> ty, subst
  | TyVariant (name, variants)  ->
      let variants', subst' =
        List.fold (fun (vts, subst) (id, ty) ->
                     first (fun ty' -> vts @ [id, ty']) (generalize ty threshold subst))
                  ([], subst) variants
      TyVariant (name, variants'), subst'
  | TyStream ty  -> first TyStream (generalize ty threshold subst)
  | TyWindow ty  -> first TyWindow (generalize ty threshold subst)
  | TyDict ty    -> first TyDict (generalize ty threshold subst)
  | TyUnknown ty -> generalize ty threshold subst // Ignore the TyUnknown part.
  | TyGen k when k < 0 ->
      let t = fresh () 
      t, (k, t)::subst
  | TyGen k when k < threshold -> ty, subst
  | TyGen k -> try
                 let t = List.assoc k subst
                 t, subst
               with err ->
                 let t = fresh ()
                 t, (k, t)::subst
  | TyArrow (t1, t2) ->
    let t1', subst' = generalize t1 threshold subst
    let t2', subst'' = generalize t2 threshold subst'
    TyArrow (t1', t2'), subst''
  | TyRecord fields ->
      let fields', subst' = Map.fold_left (fun (fields, subst) label ty ->
                                             let ty', subst' = generalize ty threshold subst
                                             Map.add label ty' fields, subst')
                                          (Map.empty, subst) fields
      TyRecord fields', subst'
  | TyTuple fields ->
      let fields', subst' = List.fold (fun (fields, subst) ty ->
                                         let ty', subst' = generalize ty threshold subst
                                         fields @ [ty'], subst')
                                      ([], subst) fields
      TyTuple fields', subst'



// dg = Don't generalize
let dg ty = (ty, System.Int32.MaxValue)


(* Analyze an expression and collect typing constraints between its elements
 * The environment contains a mapping between variables and their types. Each
 * type is annotated with the counter of the generic variable generator at the
 * time of its creation. This way, when we access a variable with type t and
 * threshold h, it is easy to find out which of t's components can be generalized
 * (those with id >= h) and which can't (those with id < h).
 * This generalization procedure allows us to obtain let-polymorphism.
 *)
let rec constr (env:TypeContext) expr =
  match expr with
  | FuncCall (fn, param) ->
      match fn with
      // Handle primitive functions
      | Id (Identifier name) when Map.contains name env && (getType env.[name]).IsPrimitive() ->
          let handler = match getType env.[name] with
                        | TyPrimitive handler -> handler
                        | _ -> failwithf "Can't happen"
          handler env param
      | _ -> let tf, cf = constr env fn
             let tp, cp = constr env param
             let ty = fresh ()
             ty, (tf, TyArrow (tp, ty))::(cf @ cp)
  | MemberAccess (target, Identifier label) ->
      match typeOf env target with
      | TyGen _ -> failwithf "Can't determine the type of the target in _____.%s. Please annotate." label
      | TyRecord fields when Map.contains label fields -> fields.[label], []
      | TyTuple elts when (int label) <= elts.Length -> elts.[(int label) - 1], []
      | other -> failwithf "The target of the _____.%s is %A" label other
  | MethodCall (target, Identifier methd, paramExprs) ->
      match typeOf env target with
      | TyGen _ -> failwithf "Can't determine the type of the target in _____.%s(). Please annotate." methd
      | other -> typeOfMethodCall env other methd paramExprs
  | ArrayIndex (target, index) ->
      match typeOf env target with
      | TyGen _ -> failwithf "Can't determine the type of the target in _____.[]. Please annotate."
      | other -> typeOfMethodCall env other "[]" [index]
  | BinaryExpr (oper, expr1, expr2) ->
      let t1, c1 = constr env expr1
      let t2, c2 = constr env expr2
      let t3, c3 = constrBinOp oper t1 t2
      t3, c1 @ c2 @ c3
  | Record fields ->
      let (types, constraints) =
        List.unzip (List.map (fun (field, expr) ->
                                let ty, ce = constr env expr
                                (field, ty), ce)
                             fields)
      TyRecord (Map.of_list types), List.concat constraints
  | Tuple exprs ->
      let (types, constraints) =
        List.unzip (List.map (fun expr ->
                                let ty, ce = constr env expr
                                ty, ce)
                             exprs)
      TyTuple types, List.concat constraints
  | Let (Id (Identifier name), None, binder, body) ->
      let tbi = fresh ()
      let prevCounter = counter () - 1  // This is the threshold
      let env' = env.Add (name, (tbi, prevCounter))
      let tbi', cbi = constr env' binder

      // Find the binder's principal type
      let subst = solve ((tbi, tbi')::cbi)
      let ptbi = typeSubst subst tbi'

      let env' = env.Add (name, (ptbi, prevCounter))
      let tbo, cbo = constr env' body
      tbo, cbi @ cbo
  | Let (Id (Identifier name), Some ty, binder, body) ->
      // This case occurs when there is pattern matching on tuples or records.
      // The type ty contains some dummy generic variables. Replace it with real
      // variables.
      let ty' = match ty with
                | TyTuple elts -> TyTuple [ for i in 1 .. elts.Length -> fresh () ]
                | TyRecord fields -> TyRecord (Map.map (fun _ _ -> fresh()) fields)
                | _ -> failwithf "Can't happen"

      // The binder is not recursive for sure.
      let tbi, cbi = constr env binder

      // We don't want to generalize ty'
      let env' = env.Add (name, dg ty')
      let tbo, cbo = constr env' body
      tbo, (ty', tbi)::(cbi @ cbo)
  | Lambda (args, expr) ->
      let expr' = genAnnot expr  // Generalize user type annotations.
  
      match expr' with
      | Lambda (args, body) ->
          let env', argTypes =
            List.fold (fun (env:TypeContext, argTypes) (Param (pattern, opty)) ->
                         let id = match pattern with
                                  | Id (Identifier id) -> id
                                  | _ -> failwithf "The argument to the lambda is a complex pattern. Rewrite was supposed to eliminate this."
                         
                         let typ = match opty with
                                   | Some t -> t
                                   | _ -> fresh ()
                         
                         // The threshold is max_int because we don't want to generalize parameters.
                         env.Add (id, (typ, System.Int32.MaxValue)), argTypes @ [typ])
                      (env, []) args

          let texp, cexp = constr env' body
          let funType = List.foldBack (fun arg acc -> TyArrow (arg, acc)) argTypes texp  //TyArrow (List.reduce (fun acc arg -> TyArrow (acc, arg)) argTypes, texp)
          funType, cexp
      | _ -> failwithf "Can't happen"          
  | If (cond, thn, els) ->
      let tc, cc = constr env cond
      let tt, ct = constr env thn
      let te, ce = constr env els
      tt, (tc, TyBool)::(tt, te)::(cc @ ct @ ce)
  | Match (expr, cases) ->
      // All cases should return the same type. Furthermore, the body of each
      // match case can be ignored because of rewrite phase.
      let te, ce = constr env expr
      let tc, cc = List.fold (fun (tc, cc) (MatchCase (pattern, _)) ->
                                let tp, cp = constr env pattern
                                tp, (tp, tc)::(cp @ cc))
                             (fresh (), [])  cases
      tc, ce @ cc
  | Seq (expr1, expr2) ->
      let t1, c1 = constr env expr1
      let t2, c2 = constr env expr2
      t2, c1 @ c2
  | Id (Identifier name) ->
      match Map.tryFind name env with
      | Some (t, threshold) ->
          // Generalize the types to obtain let-polymorphism
          fst (generalize t threshold []), []
      | None -> failwithf "constraintsOf: Not found in context - %s" name
  | Integer v -> TyInt, []
  | Float f -> TyFloat, []
  | String s -> TyString, []
  | Bool b -> TyBool, []
  | SymbolExpr _ -> TySymbol, []
  | Fail | Null -> fresh (), []
  | _ -> failwithf "Unknown expression: %A. Maybe rewrite was supposed to eliminate this?" expr


and constrBinOp oper tleft tright =
  match oper, tleft, tright with
  | Plus, TyString, _ -> TyString, []
  | Plus, _, _ -> tleft, [(tleft, tright)]
  | (Minus | Times | Div | Mod), _, _ -> TyInt, [(tleft, TyInt); (tleft, tright)]
  | (GreaterThan | GreaterThanOrEqual | Equal | NotEqual | LessThanOrEqual | LessThan), _, _ -> TyBool, [(tleft, tright)]
  | (And | Or), TyBool, TyBool -> TyBool, []
  | Is, _, TyString -> TyBool, []
  | other -> failwithf "Binary operation not implemented for these types: %A" other


and typeOfMethodCall env target name paramExps =
  failwithf "n/i"
(*
  let targetType = match typeOf env target with
                   //| TyFixed (t, _) -> t
                   | other -> other

  // Generic methods first
  match name with
  | "changes" | "updates" ->
    match paramExps with
    | [] -> TyStream (TyRecord (Map.of_list ["value", targetType])), []
    | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "count" -> TyInt, []
  | "last" | "prev" ->
      // last is applied to Stream of ev and returns an ev
      let ty = fresh ()
      ty, [SameType (targetType, TyStream ty)]
  | "sum" | "avg" | "max" | "min" ->
      // sum and avg may be applied to integers, floats, integer windows and float windows
      match targetType, paramExps with
      | _, [] -> TyInt, [SameType (targetType, freshB (Some (Set.of_list [TyInt; TyWindow TyInt])))]
      | (TyStream (TyRecord fields) | TyWindow (TyStream (TyRecord fields))), [SymbolExpr (Symbol field)] when Map.contains field fields -> fields.[field], []
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "groupby" ->
      match paramExps with
      | [SymbolExpr (Symbol field); Lambda ([Param (Id (Identifier g), _)], body) as param] ->
          match targetType with
          | TyStream _ | TyWindow (TyStream _) -> TyDict (typeOf (env.Add(g, dg targetType)) body), []
          | _ -> failwithf "Can't happen"
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "[]" ->
      match paramExps with
      | [param] when isTimeLength param -> TyWindow targetType, []
      | _ -> let ty = fresh ()
             ty, [SameType (targetType, TyDict ty)]
  | "where" ->
      match targetType, paramExps with
      | (TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType)) | TyDict argType),
          [Lambda ([Param (Id (Identifier arg), _)], expr)] ->
          if (typeOf (env.Add(arg, dg argType)) expr) <> TyBool
            then failwithf "The predicate of the where doesn't return a boolean!"
          targetType, []
      | _ -> failwithf "It was not possible to determine the type of the target in the call to %s" name
  | "select" ->
      match targetType, paramExps with
      | (TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType)) | TyDict argType),
         [Lambda ([Param (Id (Identifier arg), _)], expr)] ->
          let projty = typeOf (env.Add(arg, dg argType)) expr
          match targetType with
          | TyStream _ -> TyStream projty, []
          | TyWindow _ -> TyWindow (TyStream projty), []
          | TyDict _ -> TyDict projty, []
          | _ -> failwithf "Can't happen"
      | _ -> failwithf "It was not possible to determine the type of the target in the call to %s" name

 (*
   | _ -> match targetType with
          | TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType)) ->

          [Lambda ([Param (Id (Identifier arg), _)], expr)] ->
          if (typeOf (env.Add(arg, dg argType)) expr) <> TyBool
            then failwithf "The predicate of the where doesn't return a boolean!"
          targetType, []
      | TyDict argType, [Lambda ([Param (Id (Identifier g), _)], body) as param] ->
              let ty, cs = constr env param
              match ty with
              | TyArrow (tyg, TyBool) -> TyDict argType, SameType (argType, tyg)::cs
              | _ -> failwithf "Can't happen"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
*)

(*
  | "last" | "sum" | "max" | "min" | "avg" | "prev" ->
      match targetType, paramExps with
      | TyRecord fields, [SymbolExpr (Symbol field)] when Map.contains field fields -> fields.[field], []
      | TyStream (TyRecord fields), [SymbolExpr (Symbol field)] when Map.contains field fields -> fields.[field], []
      | TyWindow typ, _ ->
          match typ, paramExps with
   //       | TyRef typ', [SymbolExpr (Symbol field)] ->
   //           match resolveAlias env typ' with
   //           | TyType (_, fields, _, _) when Map.contains field fields -> fields.[field]
   //           | _ -> failwithf "The alias does not refer to a TyType"
          | ((*TyType (_, fields, _, _) | *)TyRecord fields | TyStream (TyRecord fields)), [SymbolExpr (Symbol field)]
              when Map.contains field fields -> fields.[field], []
          | _, [] -> TyInt, []
          | _ -> failwithf "Invalid parameters to method '%s': %A" name (typ, paramExps)
      | TyInt, [] -> TyInt, []
      | TyFloat, [] -> TyFloat, []
      | TyBool, [] -> TyBool, []
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "any?" | "all?" ->
      match targetType, paramExps with
      | (TyBool | TyWindow TyBool), [] -> TyBool, []
      | (TyStream v | TyWindow (TyStream v) | TyWindow v), [Lambda ([Param (Id (Identifier arg), _)], expr)] ->
          if (typeOf (env.Add(arg, dg v)) expr) <> TyBool
            then failwithf "The predicate of %s doesn't return a boolean!" name
          TyBool, []
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "where" ->
      match targetType, paramExps with
      | (TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType))),
          [Lambda ([Param (Id (Identifier arg), _)], expr)] ->
          if (typeOf (env.Add(arg, dg argType)) expr) <> TyBool
            then failwithf "The predicate of the where doesn't return a boolean!"
          targetType, []
      | TyDict argType, [Lambda ([Param (Id (Identifier g), _)], body) as param] ->
              let ty, cs = constr env param
              match ty with
              | TyArrow (tyg, TyBool) -> TyDict argType, SameType (argType, tyg)::cs
              | _ -> failwithf "Can't happen"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "groupby" ->
      match targetType, paramExps with
      | (TyStream (TyRecord fields as evType) | TyWindow (TyStream (TyRecord fields as evType))),
        [SymbolExpr (Symbol field); Lambda ([Param (Id (Identifier g), _)], expr)] when Map.contains field fields ->
                                                                                     TyDict (typeOf (env.Add(g, dg targetType)) expr), []
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
*)
  // Specific methods below
  | _ -> match targetType with
          (*| TyStream (TyRecord fields as evType) ->
              match name with
              | "select" ->
                  match paramExps with
                  | [Lambda ([Param (Id (Identifier ev), _)], expr)] ->
                      match typeOf (env.Add(ev, dg evType)) expr with
                      | TyRecord projFields -> TyStream (TyRecord (projFields.Add("timestamp", TyInt))), []
                      | _ -> failwithf "The projector of the select doesn't return a record!"
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" ->
                  match paramExps with
                  | [MemberAccess (Integer _, Identifier ("sec" | "min" | "hour" | "day"))] -> TyWindow targetType, []
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | _ -> failwithf "The type %A does not have method %A!" targetType name  *)
          | TyDict valueType ->
              match name with
              | "values" when paramExps = [] -> TyWindow valueType, []
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyBool ->
              match name with
              | "howLong" -> TyInt, []
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          //| TyRecord _ -> typeOf env (FuncCall (MemberAccess (target, Identifier name), [], paramExps))
          (*
          | TyWindow (TyStream evType) ->
              match name with
              | "select" ->
                  match paramExps with
                  | [Lambda ([Param (Id (Identifier ev), _)], expr)] ->
                      match typeOf (env.Add(ev, dg evType)) expr with
                      | TyRecord projFields -> TyWindow (TyStream (TyRecord (projFields.Add("timestamp", TyInt)))), []
                      | _ -> failwithf "The projector of the select doesn't return a record!"
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "added" | "expired" ->
                  match paramExps with
                  | [] -> TyStream evType, []
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "sortBy" ->
                  match paramExps with
                  | [SymbolExpr (Symbol field)] ->
                      match evType with
                      | TyRecord fields when Map.contains field fields -> TyWindow (TyStream evType), []
                      | _ -> failwithf "The type %A does not have field %A" evType field
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" -> match paramExps with
                        | [expr] when typeOf env expr = TyInt -> evType, []
                        | _ -> failwithf "Invalid index in []"
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyWindow valueType ->
              match name with
              | "added" | "expired" ->
                  match paramExps with
                  | [] -> TyStream (TyRecord (Map.of_list ["timestamp", TyInt; "value", valueType])), []
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "sort" ->
                  match paramExps with
                  | [] -> targetType, []
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "sortBy" ->
                  match paramExps with
                  | [SymbolExpr (Symbol field)] ->
                      match valueType with
                      | TyRecord fields (*| TyType (_, fields, _, _) *)when Map.contains field fields -> targetType, []
                      | _ -> failwithf "The type %A does not have field %A" valueType field
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" -> match paramExps with
                        | [expr] when typeOf env expr = TyInt -> valueType, []
                        | _ -> failwithf "Invalid index in []"
              | _ -> failwithf "The type %A does not have method %A!" targetType name
              *)
          | _ -> failwithf "The type %A does not have method %A!" targetType name
*)

and genAnnot expr =
  match expr with
  | Lambda (args, body) ->
      fst (List.fold (fun (expr, subst) (Param (pattern, opty)) ->
                        match opty with
                        | Some ty -> // maxValue ensures that only developer annotations are generalized,
                                     // not inferred types.
                                     let ty', subst' = generalize ty (System.Int32.MaxValue) subst
                                     replaceTypes subst' expr, subst'
                        | None -> expr, subst)
                    (expr, []) args)
  | _ -> failwithf "genAnnot called with a %A" expr


(*
 * Perform the given substitutions in all type annotations present in expr.
 *)  
and replaceTypes subs expr =
    match expr with
    | Lambda (args, body) ->
        let args' =
          List.map (fun (Param (pattern, opty) as arg) ->
                      match opty with
                      | Some ty  -> Param (pattern, Some (typeSubst subs ty))
                      | _ -> arg)
                   args
        Lambda (args', replaceTypes subs body)
    | _ -> visit expr (replaceTypes subs)

and types (env:TypeContext) = function
  | DefVariant (Identifier name, variants) ->
      let variantType = replaceAliases env (TyVariant (Identifier name, variants))
      let env' = env.Add(name, dg variantType)
      let env'' = List.fold (fun (env:TypeContext) (Identifier variant, meta) ->
                               let meta' = replaceAliases env meta
                               env.Add(variant, dg (TyArrow (meta', variantType))))
                            env' variants
      env''
  | Expr expr -> let typ = typeOf env expr
                 //printfn "<anonymous expr>:%A" typ
                 env
  | Def (Identifier name, expr, None) ->
      let typ = typeOf env expr
      //printfn "%s:%A" name typ
      env.Add(name, (typ, 0))
  | StreamDef (Identifier name, TyRecord fields) ->
      let fields' = Map.add "timestamp" TyInt fields
      env.Add(name, dg (TyStream (TyRecord fields')))
  | Function (Identifier name, parameters, body) ->
      types env (Def (Identifier name, Let (Id (Identifier name), None, Lambda (parameters, body), Id (Identifier name)), None))
  | other -> failwithf "Unexpected statement: %A" other


and typeOf env expr : Type =
  let ty, cns = constr env expr
  typeSubst (solve cns) ty