-- =========================================================
-- Semana 5 Sumativa 2
-- Docente: Lourdes Townsed
-- Creado por: Javiera Mülchi
-- =========================================================

SET SERVEROUTPUT ON
SET VERIFY OFF
SET FEEDBACK ON

-- =========================================================
-- CASO 0: Parametrización  
-- =========================================================
VAR b_anno NUMBER

BEGIN
  :b_anno := EXTRACT(YEAR FROM SYSDATE) - 1;
END;
/

DECLARE
  -- =========================================================
  -- CASO 0: Declaraciones obligatorias
  -- =========================================================

  TYPE t_varray_tipos IS VARRAY(2) OF VARCHAR2(50);
  v_tipos t_varray_tipos := t_varray_tipos('Avance en Efectivo',
                                          'Súper Avance en Efectivo');

  v_anno             NUMBER := :b_anno;
  v_fec_ini          DATE   := TRUNC(TO_DATE('01-01-' || v_anno, 'DD-MM-YYYY'));
  v_fec_fin_excl     DATE   := ADD_MONTHS(TRUNC(TO_DATE('01-01-' || v_anno, 'DD-MM-YYYY')), 12);

  v_total_a_procesar NUMBER := 0;
  v_procesados_det   NUMBER := 0;

  -- =========================================================
  -- CASO 4: Excepciones (3 tipos requeridos)
  -- =========================================================
  ex_sin_transacciones EXCEPTION;     
  ex_dup_pk            EXCEPTION;     
  PRAGMA EXCEPTION_INIT(ex_dup_pk, -1);

  -- =========================================================
  -- CASO 1: CURSOR DETALLE 
  -- =========================================================
  CURSOR c_detalle IS
    SELECT
      c.numrun,
      c.dvrun,
      tc.nro_tarjeta,
      t.nro_transaccion,
      t.fecha_transaccion,
      tt.nombre_tptran_tarjeta AS tipo_transaccion,
      t.monto_transaccion AS monto_transaccion   
    FROM transaccion_tarjeta_cliente t
    JOIN tarjeta_cliente tc
      ON tc.nro_tarjeta = t.nro_tarjeta
    JOIN cliente c
      ON c.numrun = tc.numrun
    JOIN tipo_transaccion_tarjeta tt
      ON tt.cod_tptran_tarjeta = t.cod_tptran_tarjeta
    WHERE t.fecha_transaccion >= v_fec_ini
      AND t.fecha_transaccion <  v_fec_fin_excl
      AND tt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
    ORDER BY t.fecha_transaccion ASC, c.numrun ASC;

  -- Registro PL/SQL  
  r_det c_detalle%ROWTYPE;

  -- =========================================================
  -- CASO 2: CURSOR RESUMEN 
  -- =========================================================
  CURSOR c_resumen(p_mes_anno VARCHAR2, p_tipo VARCHAR2) IS
    SELECT t.monto_transaccion AS monto_transaccion  
    FROM transaccion_tarjeta_cliente t
    JOIN tipo_transaccion_tarjeta tt
      ON tt.cod_tptran_tarjeta = t.cod_tptran_tarjeta
    WHERE t.fecha_transaccion >= v_fec_ini
      AND t.fecha_transaccion <  v_fec_fin_excl
      AND TO_CHAR(t.fecha_transaccion, 'MMYYYY') = p_mes_anno
      AND tt.nombre_tptran_tarjeta = p_tipo;

  -- =========================================================
  -- Función: aporte por transacción 
  -- =========================================================
  FUNCTION f_aporte_sbif(p_monto NUMBER) RETURN NUMBER IS
    v_porc   NUMBER := 0;
    v_aporte NUMBER := 0;
  BEGIN
    SELECT porc_aporte_sbif
      INTO v_porc
      FROM tramo_aporte_sbif
     WHERE p_monto BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;

    v_aporte := ROUND(p_monto * (v_porc / 100), 0);
    RETURN v_aporte;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      -- Excepción predefinida 
      RETURN 0;
  END f_aporte_sbif;

BEGIN
  DBMS_OUTPUT.PUT_LINE('== INICIO PROCESO APORTE SBIF ==');
  DBMS_OUTPUT.PUT_LINE('Año a procesar: ' || v_anno);

  -- =========================================================
  -- CASO 0: TRUNCATE  
  -- =========================================================
  EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_aporte_sbif';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_aporte_sbif';

  -- =========================================================
  -- CASO 0: Total registros a procesar  
  -- =========================================================
  SELECT COUNT(*)
    INTO v_total_a_procesar
    FROM transaccion_tarjeta_cliente t
    JOIN tipo_transaccion_tarjeta tt
      ON tt.cod_tptran_tarjeta = t.cod_tptran_tarjeta
   WHERE t.fecha_transaccion >= v_fec_ini
     AND t.fecha_transaccion <  v_fec_fin_excl
     AND tt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo');

  IF v_total_a_procesar = 0 THEN
    RAISE ex_sin_transacciones;  
  END IF;

  -- =========================================================
  -- CASO 1: Generación DETALLE
  -- =========================================================
  OPEN c_detalle;
  LOOP
    FETCH c_detalle INTO r_det;
    EXIT WHEN c_detalle%NOTFOUND;

    DECLARE
      v_monto  NUMBER;
      v_aporte NUMBER;
    BEGIN
      -- Redondeo a enteros  
      v_monto  := ROUND(r_det.monto_transaccion, 0);  
      v_aporte := f_aporte_sbif(v_monto);

      INSERT INTO detalle_aporte_sbif
        (numrun, dvrun, nro_tarjeta, nro_transaccion, fecha_transaccion,
         tipo_transaccion, monto_transaccion, aporte_sbif)  
      VALUES
        (r_det.numrun, r_det.dvrun, r_det.nro_tarjeta, r_det.nro_transaccion, r_det.fecha_transaccion,
         r_det.tipo_transaccion, v_monto, v_aporte);

      v_procesados_det := v_procesados_det + 1;
    END;
  END LOOP;
  CLOSE c_detalle;

  -- =========================================================
  -- CASO 2: Generación RESUMEN
  -- =========================================================
  FOR m IN (
    SELECT DISTINCT TO_CHAR(t.fecha_transaccion, 'MMYYYY') AS mes_anno,
                    TO_NUMBER(TO_CHAR(t.fecha_transaccion, 'YYYY')) AS y,
                    TO_NUMBER(TO_CHAR(t.fecha_transaccion, 'MM')) AS mm
      FROM transaccion_tarjeta_cliente t
      JOIN tipo_transaccion_tarjeta tt
        ON tt.cod_tptran_tarjeta = t.cod_tptran_tarjeta
     WHERE t.fecha_transaccion >= v_fec_ini
       AND t.fecha_transaccion <  v_fec_fin_excl
       AND tt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
     ORDER BY y ASC, mm ASC
  ) LOOP
    FOR i IN 1 .. v_tipos.COUNT LOOP
      DECLARE
        v_sum_monto  NUMBER := 0;
        v_sum_aporte NUMBER := 0;
        v_monto      NUMBER;
      BEGIN
        FOR r IN c_resumen(m.mes_anno, v_tipos(i)) LOOP
          v_monto      := ROUND(r.monto_transaccion, 0); 
          v_sum_monto  := v_sum_monto + v_monto;
          v_sum_aporte := v_sum_aporte + f_aporte_sbif(v_monto);
        END LOOP;

        IF v_sum_monto > 0 THEN
          INSERT INTO resumen_aporte_sbif
            (mes_anno, tipo_transaccion, monto_total_transacciones, aporte_total_abif)
          VALUES
            (m.mes_anno, v_tipos(i), ROUND(v_sum_monto, 0), ROUND(v_sum_aporte, 0));
        END IF;
      END;
    END LOOP;
  END LOOP;

  -- =========================================================
  -- CASO 3: COMMIT condicionado  
  -- =========================================================
  IF v_procesados_det = v_total_a_procesar THEN
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('OK: COMMIT. Procesados = ' || v_procesados_det || ' / ' || v_total_a_procesar);
  ELSE
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERROR: ROLLBACK. Procesados = ' || v_procesados_det ||
                         ' / Esperados = ' || v_total_a_procesar);
  END IF;

  DBMS_OUTPUT.PUT_LINE('== FIN PROCESO APORTE SBIF ==');

EXCEPTION
  WHEN ex_sin_transacciones THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Sin transacciones para el año ' || v_anno);

  WHEN ex_dup_pk THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ORA-00001 (PK/UK duplicada). Revisa truncado o claves.');

  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error inesperado: ' || SQLCODE || ' - ' || SQLERRM);
END;
/
-- =========================================================
-- Validación
-- =========================================================
SELECT COUNT(*) AS cnt_detalle FROM detalle_aporte_sbif;
SELECT COUNT(*) AS cnt_resumen FROM resumen_aporte_sbif;

SELECT * FROM detalle_aporte_sbif
ORDER BY fecha_transaccion, numrun;

SELECT * FROM resumen_aporte_sbif
ORDER BY mes_anno, tipo_transaccion;

