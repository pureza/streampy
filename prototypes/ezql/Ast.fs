#light

type prog =
  | Expr of expr
  | Def of id * expr

and expr =
  | Let of id * expr * expr
  | BinaryExpr of op * expr * expr
  | MethodCall of expr * id * expr list
  | FuncCall of expr * expr list
  | MemberAccess of expr * id
  | Lambda of id list * expr
  | If of expr * expr * expr
  | ArrayIndex of expr * expr
  | Seq of expr * expr
  | Record of (symbol * expr) list
  | RecordWith of expr * (symbol * expr) list
  | Integer of int
  | Bool of bool
  | String of string
  | Time of expr * timeUnit
  | SymbolExpr of symbol
  | Id of id

and op =
  | GreaterThan
  | GreaterThanOrEqual
  | Equal
  | NotEqual
  | LessThanOrEqual
  | LessThan
  | And
  | Or
  | Plus
  | Minus
  | Times
  | Div
  | Mod

and timeUnit = Min | Sec

and id = Identifier of string

and symbol = Symbol of string

