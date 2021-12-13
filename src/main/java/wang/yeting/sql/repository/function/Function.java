package wang.yeting.sql.repository.function;

public interface Function {
    FunctionType getType();

    FunctionHandler findHandler();

    FunctionHandler findHandler(String signature);
}
