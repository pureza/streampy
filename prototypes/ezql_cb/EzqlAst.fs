#light

type prog = Prog of stmt list

and stmt =
    | Assign of id * expr

and expr =
  | BinaryExpr of op * expr * expr
  | MethodCall of expr * id * expr list
  | FuncCall of expr * expr list
  | MemberAccess of expr * id
  | Lambda of id list * expr
  | ArrayIndex of expr * expr
  | Record of (symbol * expr) list
  | Integer of int
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
  
and timeUnit = Min | Sec  

and id = Identifier of string

and symbol = Symbol of string

