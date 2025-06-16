import pandas as pd
import numpy as np
from typing import Optional


def corr_to_cov(corr_matrix: pd.DataFrame, std_devs: pd.Series) -> pd.DataFrame:
    """
    Converts a correlation matrix to a covariance matrix.

    The formula is Cov = S * C * S, where C is the correlation matrix and S is
    a diagonal matrix of standard deviations.

    Args:
        corr_matrix (pd.DataFrame): A square DataFrame representing the
            correlation matrix. The index and columns should be identical.
        std_devs (pd.Series): A Series of standard deviations for the assets.
            Its index must contain all column names from the correlation matrix.

    Returns:
        pd.DataFrame: A DataFrame representing the covariance matrix, with the
            same index and columns as the input correlation matrix.
            
    Raises:
        ValueError: If the correlation matrix is not square, or if the
            standard deviations do not match the dimensions of the
            correlation matrix.
    """
    if not corr_matrix.index.equals(corr_matrix.columns):
        raise ValueError("Correlation matrix must have identical index and columns.")

    if corr_matrix.shape[0] != corr_matrix.shape[1]:
        raise ValueError("Correlation matrix must be square.")
    
    # Align std_devs to match the order of columns in corr_matrix
    try:
        aligned_std_devs = std_devs.loc[corr_matrix.columns]
    except KeyError:
        raise ValueError(
            "The index of the std_devs Series must contain all "
            "column names from the correlation matrix."
        )

    std_dev_matrix = np.diag(aligned_std_devs.values)
    
    cov_matrix_values = std_dev_matrix @ corr_matrix.values @ std_dev_matrix
    
    return pd.DataFrame(
        cov_matrix_values, 
        index=corr_matrix.index, 
        columns=corr_matrix.columns
    )

def find_nearest_corr(
    A: np.ndarray,
    tol: Optional[list] = None,
    max_iterations: int = 100,
    weights: Optional[np.ndarray] = None,
    except_on_too_many_iterations: bool = True,
) -> np.ndarray:
    """
    Finds the nearest correlation matrix to a symmetric matrix.

    This is a Python port of N. J. Higham's MATLAB code.

    Reference:
        N. J. Higham, Computing the nearest correlation matrix---A problem
        from finance. IMA J. Numer. Anal., 22(3):329-343, 2002.

    Args:
        A (np.ndarray): A symmetric numpy array.
        tol (Optional[list]): A convergence tolerance. Defaults to
            16*EPS. Defaults to None.
        max_iterations (int): The maximum number of iterations.
            Defaults to 100.
        weights (Optional[np.ndarray]): A vector defining a diagonal weight
            matrix diag(W). Defaults to None.
        except_on_too_many_iterations (bool): If True, raises an
            exception when iterations exceed max_iterations. If False,
            silently returns the best result found. Defaults to True.

    Returns:
        np.ndarray: The nearest correlation matrix to A.

    Raises:
        ValueError: If the input matrix is not symmetric.
        RuntimeError: If the number of iterations exceeds
            `max_iterations` and `except_on_too_many_iterations` is True.
    """
    ds = np.zeros_like(A)

    eps = np.spacing(1)
    if not np.all((np.transpose(A) == A)):
        raise ValueError("Input Matrix is not symmetric")
    if tol is None:
        tol = eps * np.shape(A)[0] * np.array([1, 1])
    if weights is None:
        weights = np.ones(np.shape(A)[0])
    X = np.copy(A)
    Y = np.copy(A)
    rel_diffY = np.inf
    rel_diffX = np.inf
    rel_diffXY = np.inf

    Whalf = np.sqrt(np.outer(weights, weights))

    iteration = 0
    while max(rel_diffX, rel_diffY, rel_diffXY) > tol[0]:
        iteration += 1
        if iteration > max_iterations:
            if except_on_too_many_iterations:
                if max_iterations == 1:
                    message = "No solution found in " + str(max_iterations) + " iteration"
                else:
                    message = "No solution found in " + str(max_iterations) + " iterations"
                raise RuntimeError(message)
            else:
                return X

        X_old = np.copy(X)
        R = X - ds
        R_wtd = Whalf * R
        X = _project_to_positive_semidefinite(R_wtd)
        X = X / Whalf
        ds = X - R
        Y_old = np.copy(Y)
        Y = np.copy(X)
        np.fill_diagonal(Y, 1)
        norm_Y = np.linalg.norm(Y, "fro")
        rel_diffX = np.linalg.norm(X - X_old, "fro") / np.linalg.norm(X, "fro")
        rel_diffY = np.linalg.norm(Y - Y_old, "fro") / norm_Y
        rel_diffXY = np.linalg.norm(Y - X, "fro") / norm_Y

        X = np.copy(Y)

    return X

def _project_to_positive_semidefinite(A: np.ndarray) -> np.ndarray:
    """
    Projects a symmetric matrix onto the cone of symmetric positive
    semi-definite matrices.

    NOTE: The input matrix is assumed to be symmetric.

    Args:
        A (np.ndarray): The symmetric matrix to project.

    Returns:
        np.ndarray: The projected matrix, which is positive semi-definite.
    """
    d, v = np.linalg.eigh(A)
    A = (v * np.maximum(d, 0)).dot(v.T)
    A = (A + A.T) / 2
    return A
