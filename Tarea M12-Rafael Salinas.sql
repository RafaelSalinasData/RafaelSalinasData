CREATE VIEW Production.vCostoProductoSimple AS
SELECT 
ProductID,Name AS NombreProducto,ListPrice AS PrecioLista
FROM 
Production.Product;


CREATE VIEW Production.vProductoConCosto AS
SELECT 
p.ProductID,
p.Name AS NombreProducto,
p.ListPrice AS PrecioLista,
pc.StandardCost AS CostoEstandar,
pc.StartDate AS FechaInicio
FROM 
Production.Product p
INNER JOIN 
Production.ProductCostHistory pc
ON p.ProductID = pc.ProductID;


CREATE FUNCTION dbo.fnCalcularDescuento
(
 @Precio DECIMAL(10, 2),
 @Descuento DECIMAL(5, 2)  
)
RETURNS DECIMAL(10, 2)
AS
BEGIN
    DECLARE @PrecioFinal DECIMAL(10, 2);

    SET @PrecioFinal = @Precio - (@Precio * @Descuento / 100);

    RETURN @PrecioFinal;
END;

CREATE FUNCTION dbo.fnLimpiarTexto
(
@Texto NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
DECLARE @Resultado NVARCHAR(MAX);

SET @Resultado = LTRIM(RTRIM(@Texto));

SET @Resultado = UPPER(@Resultado);

    SET @Resultado = REPLACE(@Resultado, '�', 'A');
    SET @Resultado = REPLACE(@Resultado, '�', 'E');
    SET @Resultado = REPLACE(@Resultado, '�', 'I');
    SET @Resultado = REPLACE(@Resultado, '�', 'O');
    SET @Resultado = REPLACE(@Resultado, '�', 'U');
    SET @Resultado = REPLACE(@Resultado, '�', 'A');
    SET @Resultado = REPLACE(@Resultado, '�', 'E');
    SET @Resultado = REPLACE(@Resultado, '�', 'I');
    SET @Resultado = REPLACE(@Resultado, '�', 'O');
    SET @Resultado = REPLACE(@Resultado, '�', 'U');
    SET @Resultado = REPLACE(@Resultado, '�', 'U');
    SET @Resultado = REPLACE(@Resultado, '�', 'N'); 

    RETURN @Resultado;
END;
SELECT dbo.fnLimpiarTexto('  Jos� P�rez  ') AS TextoLimpio;



CREATE PROCEDURE dbo.uspObtenerProductosCostosos
    @PrecioMin DECIMAL(10, 2)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        ProductID,
        Name AS NombreProducto,
        ListPrice AS PrecioLista
    FROM 
        Production.Product
    WHERE 
        ListPrice > @PrecioMin;
END;

EXEC dbo.uspObtenerProductosCostosos @PrecioMin = 1000;

SELECT dbo.fnCalcularDescuento(100.00, 15.00) AS PrecioConDescuento;