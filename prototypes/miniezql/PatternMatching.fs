open System.Collections.Generic
open Ast
open Util

exception MatchFail of string


type MatchInstr =
  | Bind of string * Type option * expr
  | Guard of expr

(*
 * Returns a list of instructions containing code that matches a given pattern.
 * These instructions can either be variable bindings that bind variables
 * mentioned in the pattern, or conditional checks that verify, on runtime,
 * the values of constants in the pattern.
 *
 * For example,
 *
 * let (a, 3) = f () returns:
 *
 * [$0 -> f (); a -> $0.1; b -> $0.2; b = 3]
 *
 * Furthermore, $0 is annotated with its expected type ('a * 'b in this case)
 * so that the type checker asserts the pattern is being used correctly.
 *
 * Note: Repeated variables are not allowed.
 *)
let patternMatch pattern binder =
  let rec patternMatch' pattern binder instr =
    match pattern, binder with
    | (Integer _ | Float _ | Bool _ | String _ | SymbolExpr _), _ ->
        // Returns a check to perform on runtime
        Guard (BinaryExpr (Equal, pattern, binder))::instr
    | Id (Identifier var), _ -> extendIfNew var binder instr
    | Tuple elts1, Tuple elts2 ->
        if elts1.Length = elts2.Length
          then List.fold (fun instr (expr1, expr2) ->
                            patternMatch' expr1 expr2 instr)
                         instr (List.zip elts1 elts2)
          else raise (MatchFail (sprintf "Can't unify %A with %A: the tuples have different lengths" pattern binder))
    | Tuple elts, _ ->
        // Create $x to contain the entire binder
        let binderId = fresh ()
        // Annotate $x with the pattern's type, so that the type checker can assert
        // the pattern corresponds to the binder's type.
        // Since, at this point, we don't know the complete type, we use dummy generic
        // types, that will be re-created for real during typechecking. For now, all that
        // matters is that this is a tuple with elts.Length elements.
        let tupleTy = TyTuple [ for i in 1 .. elts.Length -> TyGen (-i, None) ]
        List.fold (fun (instr, i) expr ->
                     patternMatch' expr (MemberAccess (Id (Identifier binderId), Identifier (string i))) instr, i + 1)
                  ((Bind (binderId, Some tupleTy, binder)::instr), 1) elts |> fst
    | Record fields1, Record fields2 ->
        // Special case: the right side is a record expression. Perform substitutions
        // directly, without creating unneeded $x variables.
        let labels1 = fields1 |> List.map fst |> Set.of_list
        let labels2 = fields2 |> List.map fst |> Set.of_list
        if labels1 = labels2
          then List.fold (fun instr (label, expr) ->
                             let expr' = List.assoc label fields2
                             patternMatch' expr expr' instr)
                         instr fields1
          else raise (MatchFail (sprintf "Can't unify %A with %A: the records have different sets of fields" pattern binder))
    | Record fields, _ ->
        // Similar to tuples. Create $x, annotate it with the pattern's type and use dummy
        // generic variables to fill in the gaps.
        let binderId = fresh ()
        let recordTy = TyRecord (Map.of_list [ for (label, _) in fields -> (label, TyGen (-1, None)) ])
        List.fold (fun instr (label, expr) ->
                     patternMatch' expr (MemberAccess (Id (Identifier binderId), Identifier label)) instr)
                  (Bind (binderId, Some recordTy, binder)::instr) fields
    | FuncCall (Id (Identifier variant), pattern), _ ->
        (*
         * match fn () with
         * | A (a, _) -> ...
         *
         * let $x = fn ()          // -> This is the Bind below
         * if $is $x A             // -> This is the Guard below
         *   then let $y = $metadata $x
         *        ...
         *)

         let binderId = fresh ()
         let instr' = [Guard (BinaryExpr (Is, Id (Identifier binderId), String variant)); Bind (binderId, None, binder)] @ instr
         patternMatch' pattern (FuncCall (Id (Identifier "$metadata"), Tuple ([String variant; Id (Identifier binderId)]))) instr'
    | If (cond, pattern, _), _ ->
         // pattern guards. The evaluation of cond must come after the pattern is matched.
         Guard cond::patternMatch' pattern binder instr
    | _ -> raise (MatchFail (sprintf "Can't unify %A with %A" pattern binder))

  and extendIfNew s t instr =
    match List.tryFind (function
                          | Bind (a, _, _) -> s = a
                          | _ -> false)
                       instr with
    | Some _ -> failwithf "No repeated variables, please."
    | _ -> if s <> "_" then Bind (s, None, t)::instr else instr

  let insns = patternMatch' pattern binder []
  List.rev insns
